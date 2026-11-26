;;;; examples/avalanche-parquet-source.lisp — streaming block-source backed by
;;;; rust/avax-parquet-source (local replay_store + local/R2 block_diffs parquet).
;;;;
;;;; Loaded via `--source examples/avalanche-parquet-source.lisp`.
;;;; Env:
;;;;   AVAX_PARQUET_BIN   (default rust/avax-parquet-source/target/release/avax-parquet-source)
;;;;   AVAX_JOURNAL_CACHE (default /root/backtests/avax-1003/journal_cache)
;;;;   AVAX_REPLAY_STORE  (default /root/backtests/avax-1003/inputs/replay_store)
;;;;   AVAX_ANCESTORS     (default data/avalanche-poc/post-sae-100/ancestor-hashes.json)

(defpackage #:eita.avalanche.parquet
  (:use #:cl #:eita.core #:eita.input))

(in-package #:eita.avalanche.parquet)

(defvar *proc* nil)
(defvar *in* nil)
(defvar *out* nil)
(defvar *lock* (sb-thread:make-mutex :name "avax-parquet-source"))
(defvar *cache* (make-hash-table :test 'eql))

(defun read-line-ascii (stream)
  "Read an LF-terminated ASCII line from a binary (unsigned-byte 8) stream."
  (let ((chars '()))
    (loop for b = (read-byte stream nil nil)
          while b
          until (= b 10)
          unless (= b 13) do (push (code-char b) chars))
    (when (or chars (listen stream))
      (coerce (nreverse chars) 'string))))

(defun read-exact-octets (stream n)
  (let ((buf (make-array n :element-type '(unsigned-byte 8)))
        (pos 0))
    (loop while (< pos n)
          for got = (read-sequence buf stream :start pos :end n)
          do (when (= got pos) (error "avax-parquet-source: unexpected EOF at ~D/~D" pos n))
             (setf pos got))
    buf))

(defun send-cmd (cmd)
  (let ((octets (sb-ext:string-to-octets (concatenate 'string cmd (string #\Newline)) :external-format :utf-8)))
    (write-sequence octets *in*)
    (finish-output *in*)))

(defun query-bytes (cmd)
  (sb-thread:with-mutex (*lock*)
    (send-cmd cmd)
    (let ((len-str (read-line-ascii *out*)))
      (unless len-str (error "avax-parquet-source exited unexpectedly"))
      (let ((len (parse-integer (string-trim '(#\Space #\Tab #\Return) len-str))))
        (when (plusp len)
          (read-exact-octets *out* len))))))

(defun start-helper! ()
  (let* ((bin (or (sb-ext:posix-getenv "AVAX_PARQUET_BIN")
                  "rust/avax-parquet-source/target/release/avax-parquet-source"))
         (jc  (or (sb-ext:posix-getenv "AVAX_JOURNAL_CACHE")
                  "/root/backtests/avax-1003/journal_cache"))
         (rs  (or (sb-ext:posix-getenv "AVAX_REPLAY_STORE")
                  "/root/backtests/avax-1003/inputs/replay_store"))
         (anc (or (sb-ext:posix-getenv "AVAX_ANCESTORS")
                  "data/avalanche-poc/post-sae-100/ancestor-hashes.json"))
         (args (list "--journal-cache" jc "--replay-store" rs "--ancestors" anc)))
    (setf *proc* (sb-ext:run-program bin args
                                     :search t
                                     :input :stream :output :stream :error *error-output*
                                     :wait nil
                                     :external-format :latin-1))
    ;; Switch streams to binary by wrapping fds
    (let ((in-fd  (sb-sys:fd-stream-fd (sb-ext:process-input *proc*)))
          (out-fd (sb-sys:fd-stream-fd (sb-ext:process-output *proc*))))
      (setf *in*  (sb-sys:make-fd-stream in-fd  :output t :element-type '(unsigned-byte 8) :buffering :full)
            *out* (sb-sys:make-fd-stream out-fd :input  t :element-type '(unsigned-byte 8) :buffering :full)))))

(defun fetch-block-parquet (n)
  (or (gethash n *cache*)
      (let ((bytes (query-bytes (format nil "B ~D" n))))
        (when bytes
          (let* ((i (eita.input::jws bytes 0))
                 (blk (or (values (eita.input::decode-block-file bytes i))
                          (values (eita.input::decode-block bytes i)))))
            (when blk
              (remhash (- n 300) *cache*)
              (setf (gethash n *cache*) blk)))))))

(defun fetch-hash-parquet (n)
  (let ((bytes (query-bytes (format nil "H ~D" n))))
    (when bytes
      (hex->int (sb-ext:octets-to-string bytes :external-format :utf-8)))))

(start-helper!)
(clrhash *cache*)
(setf *block-source*
      (make-block-source
       :name "avax-parquet-source (local+R2)"
       :block #'fetch-block-parquet
       :hash #'fetch-hash-parquet
       :prepare (lambda (lo hi)
                  (declare (ignore hi))
                  (query-bytes (format nil "P ~D" lo)))
       :forget-above (lambda (top)
                       (loop for k being the hash-keys of *cache* when (> k top) do (remhash k *cache*)))))
