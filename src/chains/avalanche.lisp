;;;; chains/avalanche.lisp — Avalanche C-Chain (Coreth + Granite + Helicon/SAE) chain spec.
;;;;
;;;; Avalanche C-Chain execution rules (avalanchego / coreth):
;;;;   - Fork: Cancun (fork ord 16) — TLOAD/TSTORE/MCOPY/BLOBBASEFEE, precompiles 0x01..0x0a,
;;;;     NO EIP-7623 calldata floor and NO EIP-7702 delegation.
;;;;   - Granite P-256 (0x100, EIP-7951 at 6900 gas): active from timestamp 1763568000 on
;;;;     mainnet (2025-11-19 16:00 UTC) and 1761750000 on Fuji (2025-10-29 15:00 UTC).
;;;;   - Pre-execution warming: Coreth's native-asset precompile addresses 0x0100...0000,
;;;;     0x0100...0001, 0x0100...0002 (plus 0x100 when Granite is active, handled by Eita's
;;;;     *p256verify* seed). Warp (0x0200...0005) is NOT prewarmed.
;;;;   - Gas refunds disabled: EVM SSTORE/SELFDESTRUCT refunds do not reduce charged gas.
;;;;   - Fee routing: whole fee (gasUsed * effectiveGasPrice) is credited to coinbase
;;;;     (0x0100...0000 on C-Chain); base fee is not burned separately.
;;;;   - Helicon / SAE (ACP-194, headers carrying `settledHeight`):
;;;;     * Executed base fee is lower than header `baseFeePerGas` (worst-case bound); derived
;;;;       from the block's dynamic-fee receipts (effectiveGasPrice - maxPriorityFeePerGas).
;;;;     * Minimum gas floor: charged = max(spent, ceil(gasLimit / 2)); gas below the floor
;;;;       is not returned to the sender and goes to coinbase.
;;;;     * Block gas capacity is accounted by tx gas limits (+ atomic gas), and consensus
;;;;       header `gasUsed` equals Σ tx.gasLimit + atomicGas; `logsBloom`/`receiptsRoot` in
;;;;       the consensus header belong to the settled block rather than this block's execution.
;;;;   - Pre-block system calls: none (no EIP-4788 / EIP-2935 code deployed on C-Chain).
;;;;   - Post-block atomic transactions (`blockExtraData`): codec v0 ImportTx (AVAX credits)
;;;;     and ExportTx (AVAX debits + nonce increments), plus atomic gas accounting.
;;;;   - Unsupported stateful Coreth precompiles (Warp 0x0200...0005, multi-coin 0x0100...0001/2)
;;;;     fail loudly instead of executing as empty accounts.
;;;;
;;;; Run: eita replay --chain avalanche --lazy --source examples/avalanche-poc-source.lisp \
;;;;        --observer examples/avalanche-poc-verifier.lisp --blocks DIR --from N --to M

(in-package #:eita.chain.avalanche)

(defconstant +fork-ord-cancun+ 16)

(defconstant +avax-mainnet-id+ 43114)
(defconstant +avax-fuji-id+ 43113)
(defconstant +granite-ts-mainnet+ 1763568000 "2025-11-19 16:00:00 UTC (avalanchego upgrade.Mainnet).")
(defconstant +granite-ts-fuji+    1761750000 "2025-10-29 15:00:00 UTC (avalanchego upgrade.Fuji).")

;;; Coreth special addresses
(defconstant +coreth-blackhole+     #x0100000000000000000000000000000000000000)
(defconstant +coreth-native-bal+    #x0100000000000000000000000000000000000001)
(defconstant +coreth-native-call+   #x0100000000000000000000000000000000000002)
(defconstant +coreth-warp+          #x0200000000000000000000000000000000000005)

;;; Avalanche mainnet AVAX asset ID (big-endian 32 bytes)
(defparameter +avax-asset-id+
  (hex->bytes "21e67317cbc4be2aeb00677ad6462778a8f52274b9d605df2591b23027a87dff"))
;;; Avalanche mainnet Warp constants (avalanchego utils/constants + graniteGasConfig)
(defconstant +avax-mainnet-network-id+ 1)
(defparameter +avax-mainnet-blockchain-id+
  (hex->bytes "0427d4b22a2a78bcddd456742caf91b56badbff985ee19aef14573e7343fd652"))
(defconstant +warp-sent-topic+
  #x56600c567728a800c0aa927500f831cb451df66a7af570eb4df4dfbf4674887d
  "keccak256(\"SendWarpMessage(address,bytes32,bytes)\").")
(defconstant +warp-sel-send+ #xee5b48eb)
(defconstant +warp-sel-blockchain-id+ #x4213cf78)
(defconstant +warp-get-blockchain-id-gas+ 200)
(defconstant +warp-send-gas+ 41500)
(defconstant +warp-send-per-byte-gas+ 8)
(defconstant +warp-max-payload-size+ (* 24 1024))
(defconstant +x2c-rate+ 1000000000 "nAVAX (1e-9) -> wei (1e-18).")
(defconstant +atomic-intrinsic-gas+ 10000)
(defconstant +atomic-gas-per-sig+ 1000)
(defconstant +atomic-codec-version-bytes+ 2)

(defstartup *avax-net* (or (sb-ext:posix-getenv "AVAX_CHAIN") "mainnet")
  "mainnet (43114) or fuji (43113).")

(defun avax-chain-id (net)
  (if (string-equal net "fuji") +avax-fuji-id+ +avax-mainnet-id+))

(defun avax-granite-ts (net)
  (if (string-equal net "fuji") +granite-ts-fuji+ +granite-ts-mainnet+))

;;; Per-block state maintained by avalanche-block-setup and consumed by avalanche-run-tx / post-block.
(defvar *avalanche-sae-block* nil "T when the current block carries an SAE `settledHeight` marker.")
(defvar *avalanche-exec-base-fee* 0 "Executed base fee for the current block (SAE: from receipts; pre-SAE: header).")
(defvar *avalanche-block-gas-limits* 0 "Running sum of tx gas limits in an SAE block.")

;;; --- SAE executed base fee from receipts ---------------------------------------------------

(defun sae-executed-base-fee (txs receipts header-base-fee block-number)
  "Derive the executed base fee of an SAE block from its dynamic-fee receipts:
   any type >= 2 tx with effectiveGasPrice < maxFeePerGas paid exactly (baseFee + maxPriorityFeePerGas).
   Verifies all such txs agree; falls back to HEADER-BASE-FEE when no tx exposes it."
  (let ((shown nil))
    (loop for tx in txs
          for r in receipts
          for typ = (or (tx-type tx) 0)
          for cap = (tx-max-fee tx)
          for tip = (or (tx-max-priority tx) 0)
          for egp = (getf r :effective-gas-price)
          when (and (>= typ 2) cap egp (< egp cap))
            do (let ((cand (- egp tip)))
                 (when (minusp cand)
                   (error "block ~D: receipt effectiveGasPrice ~D < maxPriorityFeePerGas ~D"
                          block-number egp tip))
                 (when (and shown (/= shown cand))
                   (error "block ~D: inconsistent SAE executed base fees ~D vs ~D"
                          block-number shown cand))
                 (setf shown cand)))
    (values (or shown header-base-fee 0) (not (null shown)))))

;;; --- Avalanche atomic transaction codec (v0) -----------------------------------------------

(defun decode-atomic-batch (bytes)
  "Decode Avalanche C-Chain `blockExtraData` (codec v0 ImportTx / ExportTx batch).
   Returns (values effects atomic-gas), where EFFECTS is a list of
   (:credit addr wei) or (:debit addr wei nonce-after)."
  (if (or (null bytes) (zerop (length bytes)))
      (values nil 0)
      (let ((pos 0)
            (len (length bytes))
            (effects '())
            (total-gas 0))
        (labels ((ensure-avail (n)
                   (when (> (+ pos n) len)
                     (error "atomic payload truncated: need ~D bytes at ~D/~D" n pos len)))
                 (read-u16 ()
                   (ensure-avail 2)
                   (prog1 (logior (ash (aref bytes pos) 8) (aref bytes (1+ pos)))
                     (incf pos 2)))
                 (read-u32 ()
                   (ensure-avail 4)
                   (prog1 (logior (ash (aref bytes pos) 24)
                                  (ash (aref bytes (+ pos 1)) 16)
                                  (ash (aref bytes (+ pos 2)) 8)
                                  (aref bytes (+ pos 3)))
                     (incf pos 4)))
                 (read-u64 ()
                   (ensure-avail 8)
                   (let ((v 0))
                     (dotimes (i 8) (setf v (logior (ash v 8) (aref bytes (+ pos i)))))
                     (incf pos 8)
                     v))
                 (read-addr ()
                   (ensure-avail 20)
                   (let ((v 0))
                     (dotimes (i 20) (setf v (logior (ash v 8) (aref bytes (+ pos i)))))
                     (incf pos 20)
                     v))
                 (skip (n) (ensure-avail n) (incf pos n))
                 (expect-avax-asset ()
                   (ensure-avail 32)
                   (dotimes (i 32)
                     (unless (= (aref bytes (+ pos i)) (aref +avax-asset-id+ i))
                       (error "atomic tx carries non-AVAX asset id at byte ~D" pos)))
                   (incf pos 32))
                 (skip-credentials ()
                   (let ((ncred (read-u32)))
                     (dotimes (_ ncred)
                       (let ((ctype (read-u32)))
                         (unless (= ctype 9)
                           (error "unsupported atomic credential type ~D" ctype))
                         (let ((nsigs (read-u32)))
                           (skip (* 65 nsigs)))))))
                 (decode-import ()
                   (skip (+ 4 32 32)) ; network id, blockchain id, source chain
                   (let ((n-in (read-u32)) (sigs 0))
                     (dotimes (_ n-in)
                       (skip (+ 32 4 32)) ; utxo id + asset id
                       (skip (+ 4 8))     ; input type + amount
                       (let ((ns (read-u32)))
                         (skip (* 4 ns))
                         (incf sigs ns)))
                     (let ((n-out (read-u32)))
                       (dotimes (_ n-out)
                         (let ((addr (read-addr))
                               (navax (read-u64)))
                           (expect-avax-asset)
                           (push (list :credit addr (* navax +x2c-rate+)) effects))))
                     sigs))
                 (decode-export ()
                   (skip (+ 4 32 32)) ; network id, blockchain id, destination chain
                   (let ((n-in (read-u32)))
                     (dotimes (_ n-in)
                       (let ((addr (read-addr))
                             (navax (read-u64)))
                         (expect-avax-asset)
                         (let ((nonce (read-u64)))
                           (push (list :debit addr (* navax +x2c-rate+) (1+ nonce)) effects))))
                     (let ((n-out (read-u32)))
                       (dotimes (_ n-out)
                         (skip (+ 32 4 8)) ; asset id, output type, amount
                         (skip (+ 8 4))    ; locktime, threshold
                         (let ((nowners (read-u32)))
                           (skip (* 20 nowners)))))
                     n-in)))
          (let ((version (read-u16)))
            (unless (zerop version)
              (error "unsupported atomic codec version ~D (expected 0)" version)))
          (let ((count (read-u32)))
            (dotimes (_ count)
              (let* ((unsigned-from pos)
                     (tx-type (read-u32))
                     (sigs (case tx-type
                             (0 (decode-import))
                             (1 (decode-export))
                             (otherwise (error "unknown atomic transaction type ~D" tx-type))))
                     (unsigned-bytes (- pos unsigned-from)))
                (incf total-gas (+ +atomic-intrinsic-gas+
                                   +atomic-codec-version-bytes+
                                   unsigned-bytes
                                   (* sigs +atomic-gas-per-sig+)))
                (skip-credentials))))
          (unless (= pos len)
            (error "~D trailing bytes after atomic batch" (- len pos)))
          (values (nreverse effects) total-gas)))))

;;; --- Chain hooks ---------------------------------------------------------------------------

;;; --- Coreth Warp precompile (0x0200...0005): getBlockchainID & sendWarpMessage -------------

(defun sha256-octets (bytes)
  "SHA-256 of octet vector BYTES using libeita's native %sha256-hash."
  (let ((n (length bytes))
        (out (make-array 32 :element-type '(unsigned-byte 8))))
    (with-foreign-object (out-buf :uint8 32)
      (if (plusp n)
          (with-foreign-object (in-buf :uint8 n)
            (dotimes (i n) (setf (mem-aref in-buf :uint8 i) (aref bytes i)))
            (%sha256-hash in-buf n out-buf))
          (%sha256-hash (null-pointer) 0 out-buf))
      (dotimes (i 32 out)
        (setf (aref out i) (mem-aref out-buf :uint8 i))))))

(defun abi-decode-small-word (bytes off)
  "Read big-endian uint64 from 32-byte ABI word at OFF, or NIL if upper 24 bytes are non-zero."
  (when (<= (+ off 32) (length bytes))
    (dotimes (i 24)
      (unless (zerop (aref bytes (+ off i)))
        (return-from abi-decode-small-word nil)))
    (let ((v 0))
      (loop for i from 24 below 32 do (setf v (logior (ash v 8) (aref bytes (+ off i)))))
      v)))

(defun abi-decode-bytes-arg (args)
  "Decode a single dynamic `bytes` ABI argument from ARGS, or NIL if malformed."
  (let ((offset (abi-decode-small-word args 0)))
    (when (and offset (<= (+ offset 32) (length args)))
      (let ((len (abi-decode-small-word args offset)))
        (when (and len (<= (+ offset 32 len) (length args)))
          (subseq args (+ offset 32) (+ offset 32 len)))))))

(defun warp-encode-unsigned-message (caller payload)
  "Encode avalanchego warp.UnsignedMessage wrapping payload.AddressedCall(caller, payload)."
  (let* ((caller-bytes (u256->octets caller 20))
         (ac-len (+ 2 4 4 20 4 (length payload))))
    (when (> ac-len +warp-max-payload-size+)
      (return-from warp-encode-unsigned-message nil))
    (let* ((ac (make-array ac-len :element-type '(unsigned-byte 8) :initial-element 0))
           (p 0))
      (flet ((put-u16 (buf idx v)
               (setf (aref buf idx) (ldb (byte 8 8) v)
                     (aref buf (1+ idx)) (ldb (byte 8 0) v)))
             (put-u32 (buf idx v)
               (setf (aref buf idx)       (ldb (byte 8 24) v)
                     (aref buf (+ idx 1)) (ldb (byte 8 16) v)
                     (aref buf (+ idx 2)) (ldb (byte 8 8) v)
                     (aref buf (+ idx 3)) (ldb (byte 8 0) v))))
        ;; AddressedCall: u16(0) || u32(1) || u32(20) || caller[20] || u32(len) || payload
        (put-u16 ac 0 0) (incf p 2)
        (put-u32 ac p 1) (incf p 4)
        (put-u32 ac p 20) (incf p 4)
        (replace ac caller-bytes :start1 p) (incf p 20)
        (put-u32 ac p (length payload)) (incf p 4)
        (replace ac payload :start1 p)
        ;; UnsignedMessage: u16(0) || u32(networkId) || blockchainId[32] || u32(ac-len) || ac
        (let ((msg (make-array (+ 2 4 32 4 ac-len) :element-type '(unsigned-byte 8) :initial-element 0))
              (q 0))
          (put-u16 msg 0 0) (incf q 2)
          (put-u32 msg q +avax-mainnet-network-id+) (incf q 4)
          (replace msg +avax-mainnet-blockchain-id+ :start1 q) (incf q 32)
          (put-u32 msg q ac-len) (incf q 4)
          (replace msg ac :start1 q)
          msg)))))

(defun warp-abi-encode-bytes (raw)
  "ABI-encode RAW as a single `bytes` tuple: word(32) || word(len) || raw || zero-pad."
  (let* ((len (length raw))
         (padded (* 32 (ceiling len 32)))
         (out (make-array (+ 64 padded) :element-type '(unsigned-byte 8) :initial-element 0)))
    (setf (aref out 31) 32)
    (loop for i below 8 do (setf (aref out (- 63 i)) (ldb (byte 8 (* 8 i)) len)))
    (replace out raw :start1 64)
    out))

(defun run-warp-precompile (kind caller input-ptr input-len gas is-static)
  "Execute Avalanche Granite Warp precompile (0x0200...0005) for getBlockchainID & sendWarpMessage.
   Refuses getVerifiedWarpMessage / getVerifiedWarpBlockHash loudly."
  ;; Since Granite, DELEGATECALL (2) and CALLCODE (1) revert consuming all gas.
  (when (or (= kind 1) (= kind 2))
    (return-from run-warp-precompile (values gas (make-array 0 :element-type '(unsigned-byte 8)) nil)))
  (when (< input-len 4)
    (return-from run-warp-precompile (values gas (make-array 0 :element-type '(unsigned-byte 8)) nil)))
  (let ((sel (logior (ash (mem-aref input-ptr :uint8 0) 24)
                     (ash (mem-aref input-ptr :uint8 1) 16)
                     (ash (mem-aref input-ptr :uint8 2) 8)
                     (mem-aref input-ptr :uint8 3))))
    (cond
      ((= sel +warp-sel-blockchain-id+)
       (if (> +warp-get-blockchain-id-gas+ gas)
           (values gas (make-array 0 :element-type '(unsigned-byte 8)) nil)
           (values +warp-get-blockchain-id-gas+ (copy-seq +avax-mainnet-blockchain-id+) t)))
      ((= sel +warp-sel-send+)
       (let* ((arg-len (- input-len 4))
              (cost (+ +warp-send-gas+ (* arg-len +warp-send-per-byte-gas+))))
         (when (or (> cost gas) is-static)
           (return-from run-warp-precompile (values gas (make-array 0 :element-type '(unsigned-byte 8)) nil)))
         (let ((args (make-array arg-len :element-type '(unsigned-byte 8))))
           (dotimes (i arg-len) (setf (aref args i) (mem-aref input-ptr :uint8 (+ 4 i))))
           (let* ((payload (abi-decode-bytes-arg args))
                  (unsigned (and payload (warp-encode-unsigned-message caller payload))))
             (unless unsigned
               (return-from run-warp-precompile (values gas (make-array 0 :element-type '(unsigned-byte 8)) nil)))
             (let* ((id-bytes (sha256-octets unsigned))
                    (id-int (octets-int id-bytes))
                    (log-data (warp-abi-encode-bytes unsigned)))
               (observe-log! +coreth-warp+ (list +warp-sent-topic+ caller id-int) log-data)
               (values cost id-bytes t))))))
      (t
       (error "Avalanche block ~D tx ~D: unsupported Warp precompile selector 0x~(~8,'0x~)"
              *cur-block* *cur-tx* sel)))))

(defun avalanche-precompile-p (addr)
  "Coreth stateful precompiles: Warp (0x0200...0005) and native multi-coin (0x0100...0001/2)."
  (or (= addr +coreth-warp+)
      (= addr +coreth-native-bal+)
      (= addr +coreth-native-call+)))

(defun avalanche-precompile-run (addr kind caller input-ptr input-len gas is-static &optional value)
  (declare (ignore value))
  (if (= addr +coreth-warp+)
      (run-warp-precompile kind caller input-ptr input-len gas is-static)
      (error "Avalanche block ~D tx ~D: unsupported Coreth stateful precompile ~A"
             *cur-block* *cur-tx* (addr-hex addr))))

(defun avalanche-tx-prewarm! (from to value)
  "Warm Coreth's active native-asset precompiles at tx start (RulesExtra.ActivePrecompiles).
   P-256 (0x100) is warmed automatically by Eita's precompile seed when *p256verify* is T."
  (declare (ignore from to value))
  (access-addr! +coreth-blackhole+)
  (access-addr! +coreth-native-bal+)
  (access-addr! +coreth-native-call+))

(defun avalanche-block-setup (net)
  (let ((granite-ts (avax-granite-ts net)))
    (lambda (n blk ts)
      (unless (= *fork-ord* +fork-ord-cancun+)
        (setf *fork-ord* +fork-ord-cancun+)
        (%set-fork +fork-ord-cancun+))
      (setf *el-fork-ord* nil
            *p256verify* (>= ts granite-ts))
      (let* ((sae (not (null (blk-settled-height blk))))
             (ext-gas (or (blk-ext-data-gas-used blk) 0))
             (hdr-bf (or (blk-base-fee blk) 0))
             (receipts (when sae (or (blk-receipts blk) (and *block-source* (fetch-receipts n))))))
        (setf *avalanche-sae-block* sae
              *avalanche-block-gas-limits* 0
              *avalanche-exec-base-fee*
              (if sae
                  (sae-executed-base-fee (blk-txs blk) receipts hdr-bf n)
                  hdr-bf)
              *chain-header-foreign*
              (cond (sae
                     '("gasUsed" "logsBloom" "receiptsRoot" "withdrawalsRoot" "requestsHash" "blobGasUsed"))
                    ((plusp ext-gas)
                     '("gasUsed" "withdrawalsRoot" "requestsHash" "blobGasUsed"))
                    (t
                     '("withdrawalsRoot" "requestsHash" "blobGasUsed"))))))))

(defun avalanche-pre-block! (world n blk coinbase block-ctx)
  "No EIP-4788 or EIP-2935 system contracts are deployed on the Avalanche C-Chain."
  (declare (ignore world n blk coinbase block-ctx))
  nil)

(defun avalanche-post-block! (world n blk total-gas coinbase block-ctx)
  "Apply Avalanche atomic ImportTx/ExportTx effects and verify SAE / atomic header gas."
  (declare (ignore coinbase block-ctx))
  (multiple-value-bind (effects atomic-gas) (decode-atomic-batch (blk-block-extra-data blk))
    (when effects
      (observe-tx-begin!)
      (dolist (eff effects)
        (ecase (first eff)
          (:credit
           (destructuring-bind (_ addr wei) eff
             (declare (ignore _))
             (fault-account! addr)
             (balance-delta! world addr wei)))
          (:debit
           (destructuring-bind (_ addr wei nonce-after) eff
             (declare (ignore _))
             (fault-account! addr)
             (balance-delta! world addr (- wei))
             (setf (nonce-of world addr) nonce-after)))))
      (observe-tx-end! n +postblock-tx+ t))
    ;; Verify header gasUsed when it was delegated out of Ethereum's verify-block-header:
    ;; - SAE: consensus header gasUsed = sum(tx.gasLimit) + atomicGas
    ;; - Pre-SAE with atomic gas: header gasUsed = sum(tx.gasUsed) + extDataGasUsed
    (cond (*avalanche-sae-block*
           (gate n :header "SAE gasUsed (limits + atomic)"
                 (blk-gas-used blk) (+ *avalanche-block-gas-limits* atomic-gas)))
          ((plusp (or (blk-ext-data-gas-used blk) 0))
           (gate n :header "pre-SAE gasUsed (txs + atomic)"
                 (blk-gas-used blk) (+ total-gas atomic-gas))))))

(defun avalanche-run-tx (world tx receipt block-ctx coinbase blk idx)
  "Execute one Avalanche C-Chain user transaction (Coreth + Helicon/SAE rules):
   - Uses executed base fee (SAE: from receipts; pre-SAE: header) for both EGP and BASEFEE.
   - SAE block gas allowance tracks tx gas limits; pre-SAE tracks cumulative gas used.
   - Disables EVM gas refunds (Coreth post-Apricot).
   - Under SAE, enforces minimum gas consumption floor: charged = max(spent, ceil(gasLimit/2)).
   - Credits the entire fee (charged * effectiveGasPrice) to coinbase."
  (declare (ignore world))
  (let* ((from (tx-sender tx))
         (gas-limit (tx-gas-limit tx))
         (to (tx-to tx))
         (value (tx-value tx))
         (base-fee *avalanche-exec-base-fee*)
         (tx-block-ctx (list* :base-fee base-fee block-ctx))
         (egp (effective-gas-price tx base-fee))
         (block-gas-limit (getf block-ctx :gas-limit))
         (used-so-far (if *avalanche-sae-block* *avalanche-block-gas-limits* *receipt-cumulative-gas*))
         (left (- block-gas-limit used-so-far)))
    (when (> gas-limit left)
      (invalid! "gas allowance"
                "blk=~D tx=~D is invalid: gas limit ~D above the block's ~D left"
                blk idx gas-limit left))
    (when *avalanche-sae-block*
      (incf *avalanche-block-gas-limits* gas-limit))
    (observe-tx-begin!)
    (fault-account! from)
    (when (and receipt (getf receipt :effective-gas-price))
      (gate blk idx "effectiveGasPrice" (getf receipt :effective-gas-price) egp))
    (reset-tx-state gas-limit)
    (when *chain-tx-prewarm-hook*
      (funcall *chain-tx-prewarm-hook*
               from (or to (derive-create-address from (nonce-of *current-world* from))) value))
    (multiple-value-bind (success exec-used rej)
        (apply-tx tx coinbase tx-block-ctx :egp egp)
      (unless (zerop rej)
        (invalid! (aref +tx-invalid-reasons+ (1- rej))
                  "blk=~D tx=~D is invalid: ~A (sender nonce ~D balance ~D)"
                  blk idx (aref +tx-invalid-reasons+ (1- rej))
                  (nonce-of *current-world* from) (balance-of *current-world* from)))
      ;; Coreth disables EVM gas refunds; SAE enforces ceil(gasLimit / 2) floor.
      (let* ((spent (+ (intrinsic-gas tx) exec-used))
             (gas-used (if *avalanche-sae-block*
                           (max spent (ceiling gas-limit 2))
                           spent)))
        ;; Unused gas back to sender; full fee (base + tip) to coinbase.
        (spec-gas-refund-effect from gas-limit gas-used egp)
        (spec-coinbase-fee-effect coinbase gas-used egp 0)
        (spec-destroy-effect)
        (spec-sweep-effect)
        (observe-tx-end! blk idx success)
        (values gas-used (if success 1 0) egp)))))

(defun avalanche-chain-spec (&optional (net *avax-net*))
  "Avalanche C-Chain as a chain-spec (mainnet 43114 or fuji 43113)."
  (let ((fuji (string-equal net "fuji")))
    (make-chain-spec
     :name (if fuji "Avalanche C-Chain Fuji" "Avalanche C-Chain Mainnet")
     :id (avax-chain-id net)
     :banner "fork=Cancun(16) + Granite P256 + Coreth/SAE"
     :reorg-depth 0
     :rpc-url (or (sb-ext:posix-getenv "AVAX_RPC_URL")
                  (if fuji "https://api.avax-test.network/ext/bc/C/rpc"
                           "https://api.avax.network/ext/bc/C/rpc"))
     :skip-eth-header-hash t
     :block-setup (avalanche-block-setup net)
     :pre-block #'avalanche-pre-block!
     :post-block #'avalanche-post-block!
     :run-tx #'avalanche-run-tx
     :tx-prewarm #'avalanche-tx-prewarm!
     :precompile-p #'avalanche-precompile-p
     :precompile-run #'avalanche-precompile-run
     :header-foreign '("withdrawalsRoot" "requestsHash" "blobGasUsed")
     :observe-kinds '(:storage_diff :account_diff :transfer :log :tx :block))))

(register-chain-spec "avalanche" (lambda () (avalanche-chain-spec "mainnet")))
(register-chain-spec "avalanche-fuji" (lambda () (avalanche-chain-spec "fuji")))

(defun main-avalanche (args &optional (net *avax-net*))
  "Install Avalanche C-Chain (mainnet or fuji), then run the generic driver."
  (setf *avax-net* net)
  (install-chain-spec! (avalanche-chain-spec net))
  (replay-main args))
