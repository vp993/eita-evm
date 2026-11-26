;;;; examples/avalanche-poc-verifier.lisp — deep differential verifier observer for Avalanche C-Chain PoC.
;;;;
;;;; Installed via --observer examples/avalanche-poc-verifier.lisp (alongside avalanche-poc-source.lisp).
;;;; On EVERY transaction of EVERY block, verifies against the archive node's ground truth:
;;;;   1. Every emitted log (count, address, topics, data) vs eth_getBlockReceipts.
;;;;   2. Every modified account (balance, nonce, codeHash) vs debug_traceBlockByNumber (prestateTracer diffMode:true).
;;;;   3. Every modified storage slot (key -> post value, including deletions to 0) vs prestateTracer diffMode:true.
;;;;
;;;; Also reports wall time, total gas, throughput (MGas/s), and exact mismatch counts.

(defpackage #:eita.avalanche.verifier
  (:use #:cl #:eita.core #:eita.evm #:eita.state #:eita.input))

(in-package #:eita.avalanche.verifier)

(defun parse-hex-int (v)
  (cond ((null v) 0)
        ((integerp v) v)
        ((stringp v) (if (zerop (length v)) 0 (hex->int v)))
        (t 0)))

(defun parse-addr (s)
  (hex->int s))

(defun block-meta (n)
  (funcall (symbol-function (find-symbol "BLOCK-META" "EITA.AVALANCHE.POC")) n))

(defvar *v-t0* 0)
(defvar *v-blocks* 0)
(defvar *v-txs* 0)
(defvar *v-gas* 0)
(defvar *v-logs-checked* 0)
(defvar *v-log-mismatches* 0)
(defvar *v-accts-checked* 0)
(defvar *v-acct-mismatches* 0)
(defvar *v-slots-checked* 0)
(defvar *v-slot-mismatches* 0)
(defvar *v-first-errors* '())

(defun record-mismatch (fmt &rest args)
  (let ((msg (apply #'format nil fmt args)))
    (when (< (length *v-first-errors*) 20)
      (push msg *v-first-errors*))
    (format t "~&[VERIFY MISMATCH] ~A~%" msg)))

(defun empty-code-hash-int ()
  (octets-int (keccak256-bytes (make-array 0 :element-type '(unsigned-byte 8)))))

(defun verify-tx-logs (blk idx success raw-receipt)
  "Compare committed logs of (BLK, IDX) against RAW-RECEIPT's `logs`."
  (let ((eita-logs '()))
    (when success
      (eita.evm::do-tx-events (:transfer (kind depth from to value) :log (lidx addr topics data))
        (:transfer (declare (ignore kind depth from to value)))
        (:log (declare (ignore lidx))
              (push (list addr topics data) eita-logs))))
    (setf eita-logs (nreverse eita-logs))
    (let* ((chain-logs (or (and raw-receipt (jsown:val-safe raw-receipt "logs")) '())))
      (if (/= (length eita-logs) (length chain-logs))
          (progn
            (incf *v-log-mismatches*)
            (record-mismatch "blk=~D tx=~D log count: eita=~D chain=~D"
                             blk idx (length eita-logs) (length chain-logs)))
          (loop for el in eita-logs
                for cl in chain-logs
                for li from 0
                do (incf *v-logs-checked*)
                   (let* ((e-addr (first el))
                          (e-topics (second el))
                          (e-data (third el))
                          (c-addr (parse-addr (jsown:val cl "address")))
                          (c-topics (mapcar #'parse-hex-int (jsown:val cl "topics")))
                          (c-data-str (jsown:val cl "data"))
                          (c-data (if (and c-data-str (> (length c-data-str) 2))
                                      (hex->bytes c-data-str)
                                      (make-array 0 :element-type '(unsigned-byte 8)))))
                     (unless (and (= e-addr c-addr)
                                  (equal e-topics c-topics)
                                  (equalp e-data c-data))
                       (incf *v-log-mismatches*)
                       (record-mismatch "blk=~D tx=~D log ~D differs (addr ~A vs ~A)"
                                        blk idx li (addr-hex e-addr) (addr-hex c-addr)))))))))

(defun verify-tx-state (blk idx raw-diff)
  "Compare Eita's net account + storage changes AND resulting world state against prestateTracer diffMode:true."
  (when (and raw-diff (listp raw-diff) (eq (first raw-diff) :obj))
    (let* ((w *current-world*)
           (raw-pre (jsown:val-safe raw-diff "pre"))
           (raw-post (jsown:val-safe raw-diff "post"))
           (chain-pre (make-hash-table :test 'eql))
           (chain-post (make-hash-table :test 'eql))
           (eita-accts (make-hash-table :test 'eql))
           (eita-slots (make-hash-table :test 'equal)))
      ;; Index chain pre & post maps
      (when (and raw-pre (listp raw-pre) (eq (first raw-pre) :obj))
        (dolist (p (rest raw-pre)) (setf (gethash (parse-addr (car p)) chain-pre) (cdr p))))
      (when (and raw-post (listp raw-post) (eq (first raw-post) :obj))
        (dolist (p (rest raw-post)) (setf (gethash (parse-addr (car p)) chain-post) (cdr p))))
      ;; Collect Eita's observed net account and storage changes
      (eita.evm::do-account-changes (addr n0 n1 b0 b1 h0 h1)
        (setf (gethash addr eita-accts) (list n0 n1 b0 b1 h0 h1)))
      (eita.evm::do-storage-changes (addr slot pre post)
        (setf (gethash (cons addr slot) eita-slots) (cons pre post)))
      ;; 1. Check every account in chain post (balance, nonce, code, storage)
      (maphash
       (lambda (addr post-obj)
         (incf *v-accts-checked*)
         (let ((pre-obj (gethash addr chain-pre))
               (e-bal (balance-of w addr))
               (e-nonce (nonce-of w addr))
               (e-code (code-of w addr)))
           ;; Balance check when stated in post
           (when (jsown:keyp post-obj "balance")
             (let ((c-bal (parse-hex-int (jsown:val post-obj "balance"))))
               (unless (= e-bal c-bal)
                 (incf *v-acct-mismatches*)
                 (record-mismatch "blk=~D tx=~D acct ~A balance: eita=~D chain=~D (Δ=~D)"
                                  blk idx (addr-hex addr) e-bal c-bal (- e-bal c-bal)))))
           ;; Nonce check when stated in post
           (when (jsown:keyp post-obj "nonce")
             (let ((c-nonce (parse-hex-int (jsown:val post-obj "nonce"))))
               (unless (= e-nonce c-nonce)
                 (incf *v-acct-mismatches*)
                 (record-mismatch "blk=~D tx=~D acct ~A nonce: eita=~D chain=~D"
                                  blk idx (addr-hex addr) e-nonce c-nonce))))
           ;; Code check when stated in post
           (when (jsown:keyp post-obj "code")
             (let* ((cs (jsown:val post-obj "code"))
                    (c-code (if (and cs (> (length cs) 2))
                                (hex->bytes cs)
                                (make-array 0 :element-type '(unsigned-byte 8)))))
               (unless (equalp e-code c-code)
                 (incf *v-acct-mismatches*)
                 (record-mismatch "blk=~D tx=~D acct ~A code length: eita=~D chain=~D"
                                  blk idx (addr-hex addr) (length e-code) (length c-code)))))
           ;; Storage slots written in post
           (let ((post-stor (jsown:val-safe post-obj "storage"))
                 (checked-slots (make-hash-table :test 'eql)))
             (when (and post-stor (listp post-stor) (eq (first post-stor) :obj))
               (dolist (sp (rest post-stor))
                 (let* ((sk (parse-hex-int (car sp)))
                        (cv (parse-hex-int (cdr sp)))
                        (ev (world-storage-get w addr sk)))
                   (setf (gethash sk checked-slots) t)
                   (incf *v-slots-checked*)
                   (unless (= ev cv)
                     (incf *v-slot-mismatches*)
                     (record-mismatch "blk=~D tx=~D acct ~A slot 0x~(~64,'0x~): eita=0x~(~x~) chain=0x~(~x~)"
                                      blk idx (addr-hex addr) sk ev cv)))))
             ;; Storage slots deleted to 0 (present in pre.storage, absent from post.storage)
             (let ((pre-stor (and pre-obj (jsown:val-safe pre-obj "storage"))))
               (when (and pre-stor (listp pre-stor) (eq (first pre-stor) :obj))
                 (dolist (sp (rest pre-stor))
                   (let ((sk (parse-hex-int (car sp))))
                     (unless (gethash sk checked-slots)
                       (let ((ev (world-storage-get w addr sk)))
                         (incf *v-slots-checked*)
                         (unless (zerop ev)
                           (incf *v-slot-mismatches*)
                           (record-mismatch "blk=~D tx=~D acct ~A deleted slot 0x~(~64,'0x~): eita=0x~(~x~) chain=0"
                                            blk idx (addr-hex addr) sk ev)))))))))))
       chain-post)
      ;; 2. Reverse check: any slot Eita changed must be in chain-pre or chain-post!
      (maphash
       (lambda (k ev)
         (let* ((addr (car k))
                (sk (cdr k))
                (post-obj (gethash addr chain-post))
                (pre-obj (gethash addr chain-pre))
                (post-stor (and post-obj (jsown:val-safe post-obj "storage")))
                (pre-stor (and pre-obj (jsown:val-safe pre-obj "storage")))
                (in-post (and post-stor (listp post-stor)
                              (some (lambda (sp) (= (parse-hex-int (car sp)) sk)) (rest post-stor))))
                (in-pre (and pre-stor (listp pre-stor)
                             (some (lambda (sp) (= (parse-hex-int (car sp)) sk)) (rest pre-stor)))))
           (unless (or in-post in-pre)
             (incf *v-slot-mismatches*)
             (record-mismatch "blk=~D tx=~D EXTRA eita slot write acct ~A slot 0x~(~64,'0x~) val=0x~(~x~) not in chain diff"
                              blk idx (addr-hex addr) sk ev))))
       eita-slots)
      ;; 3. Reverse check: any account Eita changed (balance/nonce/code) must appear in chain-post or chain-pre!
      (maphash
       (lambda (addr fields)
         (unless (or (gethash addr chain-post) (gethash addr chain-pre))
           (incf *v-acct-mismatches*)
           (record-mismatch "blk=~D tx=~D EXTRA eita account change ~A (nonce=~D bal=~D) not in chain diff"
                            blk idx (addr-hex addr) (first fields) (second fields))))
       eita-accts))))

(defun install-verifier! ()
  (setf *v-t0* (get-internal-real-time)
        *v-blocks* 0 *v-txs* 0 *v-gas* 0
        *v-logs-checked* 0 *v-log-mismatches* 0
        *v-accts-checked* 0 *v-acct-mismatches* 0
        *v-slots-checked* 0 *v-slot-mismatches* 0
        *v-first-errors* '())
  (setf *observer*
        (make-observer
         :name "avalanche-poc-verifier"
         :block (lambda (blk fork-ord)
                  (declare (ignore blk fork-ord))
                  (incf *v-blocks*))
         :tx-end (lambda (blk idx success)
                   (when (>= idx 0)
                     (let* ((meta (block-meta blk))
                            (recs (getf meta :receipts))
                            (diffs (getf meta :diffs))
                            (raw-rec (and (< idx (length recs)) (aref recs idx)))
                            (raw-diff (and (< idx (length diffs)) (aref diffs idx))))
                       (verify-tx-logs blk idx success raw-rec)
                       (verify-tx-state blk idx raw-diff))))
         :tx-result (lambda (n idx tx gas-used status egp)
                      (declare (ignore n idx tx status egp))
                      (incf *v-txs*)
                      (incf *v-gas* gas-used))
         :attach (lambda ()
                   (attach-subscriber!
                    "avalanche-poc-verifier-summary"
                    (lambda (ev)
                      (when (eq (first ev) :finalize)
                        (let* ((dt (/ (- (get-internal-real-time) *v-t0*)
                                      internal-time-units-per-second))
                               (mgas (if (plusp dt) (/ *v-gas* dt 1d6) 0.0d0))
                               (total-err (+ *v-log-mismatches* *v-acct-mismatches* *v-slot-mismatches*
                                             (getf *stats* :mismatches))))
                          (format t "~&~%=== AVALANCHE POC VERIFICATION REPORT ===~%")
                          (format t "  blocks:           ~D~%" *v-blocks*)
                          (format t "  txs:              ~D~%" *v-txs*)
                          (format t "  total gas:        ~D (~,2F MGas/s over ~,3Fs)~%" *v-gas* mgas dt)
                          (format t "  gate mismatches:  ~D (gasUsed / status / EGP / header)~%" (getf *stats* :mismatches))
                          (format t "  logs checked:     ~D (mismatches: ~D)~%" *v-logs-checked* *v-log-mismatches*)
                          (format t "  accounts checked: ~D (mismatches: ~D)~%" *v-accts-checked* *v-acct-mismatches*)
                          (format t "  slots checked:    ~D (mismatches: ~D)~%" *v-slots-checked* *v-slot-mismatches*)
                          (format t "  VERDICT:          ~A~%"
                                  (if (and (plusp *v-blocks*) (zerop total-err)) "PASS (100% BIT-EXACT)" "FAIL"))
                          (force-output)
                          (when (or (zerop *v-blocks*) (plusp total-err))
                            (sb-ext:exit :code 1))))))))))

(install-verifier!)
