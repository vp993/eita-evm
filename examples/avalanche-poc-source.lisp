;;;; examples/avalanche-poc-source.lisp — lazy prestate-backed block source for Avalanche C-Chain PoC.
;;;;
;;;; Reads a directory produced by scripts/fetch-avalanche-poc.py:
;;;;   <dir>/ancestor-hashes.json   BLOCKHASH window [start-256 .. start-1]
;;;;   <dir>/<n>.json               {"block": ..., "receipts": ...}
;;;;   <dir>/<n>.meta.json          {"receipts": ..., "prestate": [...], "diff": [...]}
;;;;
;;;; Each account and storage slot is faulted ONCE on first touch across the entire window
;;;; (using the current tx's prestateTracer pre-image; because every write faults first and
;;;; *keep-zero-slots* is T, state carries continuously from block N to N+1 with zero resets).
;;;;
;;;; Exports *avax-poc-meta-fn* so examples/avalanche-poc-verifier.lisp can read the same
;;;; parsed ground-truth receipts and diff traces without re-parsing JSON.

(defpackage #:eita.avalanche.poc
  (:use #:cl #:eita.core #:eita.evm #:eita.state #:eita.input)
  (:export #:block-meta #:parse-hex-int #:parse-addr))

(in-package #:eita.avalanche.poc)

(defun parse-hex-int (v)
  (cond ((null v) 0)
        ((integerp v) v)
        ((stringp v) (if (zerop (length v)) 0 (hex->int v)))
        (t 0)))

(defun parse-addr (s)
  (hex->int s))

(defun normalize-prestate-map (raw-pre)
  "Convert a jsown prestate object {addr -> {balance, nonce, code, storage}} into a hash table
   addr-int -> (balance nonce code-bytes storage-ht)."
  (let ((ht (make-hash-table :test 'eql)))
    (when (and raw-pre (listp raw-pre) (eq (first raw-pre) :obj))
      (dolist (pair (rest raw-pre))
        (let* ((addr (parse-addr (car pair)))
               (obj (cdr pair))
               (bal (parse-hex-int (jsown:val-safe obj "balance")))
               (nonce (parse-hex-int (jsown:val-safe obj "nonce")))
               (code-str (jsown:val-safe obj "code"))
               (code (if (and code-str (> (length code-str) 2))
                         (hex->bytes code-str)
                         (make-array 0 :element-type '(unsigned-byte 8))))
               (raw-stor (jsown:val-safe obj "storage"))
               (stor (make-hash-table :test 'eql)))
          (when (and raw-stor (listp raw-stor) (eq (first raw-stor) :obj))
            (dolist (sp (rest raw-stor))
              (setf (gethash (parse-hex-int (car sp)) stor)
                    (parse-hex-int (cdr sp)))))
          (setf (gethash addr ht) (list bal nonce code stor)))))
    ht))

(defvar *meta-cache* (make-hash-table :test 'eql)
  "block-number -> (:receipts raw-receipts :prestates vector-of-ht :diffs vector-of-raw)")
(defvar *poc-dir* nil)

(defun load-block-meta (n)
  (or (gethash n *meta-cache*)
      (let* ((path (merge-pathnames (format nil "~D.meta.json" n) *poc-dir*))
             (parsed (jsown:parse (alexandria:read-file-into-string path)))
             (receipts (coerce (or (jsown:val-safe parsed "receipts") '()) 'vector))
             (raw-pre (or (jsown:val-safe parsed "prestate") '()))
             (raw-diff (or (jsown:val-safe parsed "diff") '()))
             (prestates (coerce (mapcar (lambda (entry)
                                          (normalize-prestate-map (jsown:val-safe entry "result")))
                                        raw-pre)
                                'vector))
             (diffs (coerce (mapcar (lambda (entry)
                                      (jsown:val-safe entry "result"))
                                    raw-diff)
                            'vector))
             (meta (list :receipts receipts :prestates prestates :diffs diffs)))
        (remhash (- n 4) *meta-cache*)
        (setf (gethash n *meta-cache*) meta))))

(defun block-meta (n)
  (load-block-meta n))

(defun current-tx-prestate ()
  "The normalized prestate hash table for (*cur-block*, *cur-tx*), or NIL outside a user tx."
  (when (and (integerp *cur-block*) (integerp *cur-tx*) (>= *cur-tx* 0))
    (let* ((meta (load-block-meta *cur-block*))
           (pv (getf meta :prestates)))
      (when (< *cur-tx* (length pv))
        (aref pv *cur-tx*)))))

(defun install-avalanche-poc-source! (dir)
  (let* ((dir-str (namestring dir))
         (dir-path (pathname (if (char= (char dir-str (1- (length dir-str))) #\/)
                                 dir-str
                                 (concatenate 'string dir-str "/"))))
         (file-src (directory-block-source dir-path :name (format nil "avalanche-poc (~A)" dir-str)))
         (ancestors (make-hash-table :test 'eql)))
    (setf *poc-dir* dir-path)
    (clrhash *meta-cache*)
    ;; Load ancestor hashes for BLOCKHASH opcode
    (let ((anc-path (merge-pathnames "ancestor-hashes.json" dir-path)))
      (when (probe-file anc-path)
        (let ((obj (jsown:parse (alexandria:read-file-into-string anc-path))))
          (dolist (pair (rest obj))
            (setf (gethash (parse-integer (car pair)) ancestors)
                  (parse-hex-int (cdr pair)))))))
    (setf *block-source*
          (make-block-source
           :name (format nil "avalanche-poc (~A)" dir-str)
           :begin (lambda (start)
                    (declare (ignore start))
                    ;; Keep zero slots in native slot tables so a slot written to 0 in block N
                    ;; is never re-faulted from prestate in block N+1.
                    (setf *keep-zero-slots* t))
           :block (lambda (n)
                    (load-block-meta n)
                    (funcall (eita.input::block-source-block file-src) n))
           :hash (lambda (n)
                   (or (gethash n ancestors)
                       (let ((b (funcall (eita.input::block-source-block file-src) n)))
                         (and b (blk-hash b)))))
           :fault-account (lambda (addr)
                            (let* ((pre (current-tx-prestate))
                                   (entry (and pre (gethash addr pre))))
                              (when entry
                                (destructuring-bind (bal nonce code stor) entry
                                  (declare (ignore stor))
                                  (let ((w *current-world*))
                                    (setf (balance-of w addr) bal
                                          (nonce-of w addr) nonce)
                                    (when (plusp (length code))
                                      (setf (code-of w addr) code))
                                    (when (or (plusp bal) (plusp nonce) (plusp (length code)))
                                      (account-mark! w addr)))))))
           :fault-slot (lambda (addr slot)
                         (let* ((pre (current-tx-prestate))
                                (entry (and pre (gethash addr pre)))
                                (stor (and entry (fourth entry)))
                                (val (or (and stor (gethash slot stor)) 0)))
                           ;; Always write into the world (including 0, kept because *keep-zero-slots* = T)
                           (world-storage-set *current-world* addr slot val)))))))

(let ((dir (or (sb-ext:posix-getenv "EITA_BLOCKS_DIR")
               (error "avalanche-poc-source: EITA_BLOCKS_DIR (--blocks) is required"))))
  (install-avalanche-poc-source! dir))
