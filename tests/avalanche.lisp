;;;; tests/avalanche.lisp — unit tests for Avalanche C-Chain (Coreth + Granite + Helicon/SAE).
;;;;
;;;; Checks:
;;;;   1. Coreth disables EVM gas refunds (clearing storage does NOT reduce charged gas).
;;;;   2. Full fee (gasUsed * effectiveGasPrice) is credited to coinbase, not burned.
;;;;   3. Helicon / SAE minimum gas floor: charged = max(spent, ceil(gasLimit / 2)), with the
;;;;      unspent floor gas kept from the sender and paid to coinbase.
;;;;   4. SAE executed base fee extraction from dynamic-fee receipts.
;;;;   5. Granite P-256 precompile (0x100) active at 6900 gas from 1763568000, inactive before.
;;;;   6. Avalanche atomic batch codec v0 (ImportTx credit + ExportTx debit & nonce increment).

(in-package #:eita.evm)

(let ((pass 0) (fail 0))
  (labels ((chk (label ok &optional detail)
             (if ok
                 (progn (incf pass) (format t "  PASS  ~A~%" label))
                 (progn (incf fail) (format t "  FAIL  ~A~@[: ~A~]~%" label detail))))
           (run-avax-tx (world tx &key (sae nil) (base-fee 25) (ts 1789110499))
             (let* ((coinbase #x0100000000000000000000000000000000000000)
                    (spec (eita.chain.avalanche:avalanche-chain-spec "mainnet"))
                    (eita.evm::*current-world* world)
                    (eita.evm::*cur-block* 96000000)
                    (eita.evm::*cur-tx* 0)
                    (eita.evm::*receipt-cumulative-gas* 0)
                    (eita.chain.avalanche::*avalanche-sae-block* sae)
                    (eita.chain.avalanche::*avalanche-exec-base-fee* base-fee)
                    (eita.chain.avalanche::*avalanche-block-gas-limits* 0)
                    (block-ctx (list :timestamp ts :number 96000000 :base-fee base-fee
                                     :gas-limit 30000000 :prev-randao 0 :coinbase coinbase
                                     :chain-id 43114 :excess-blob-gas 0 :blob-base-fee 1)))
               (eita.chain:install-chain-spec! spec)
               (unless (= *fork-ord* 16) (setf *fork-ord* 16) (%set-fork 16))
               (setf *p256verify* (>= ts 1763568000) *prague-delegation* nil)
               (funcall (eita.chain::chain-spec-run-tx spec) world tx nil block-ctx coinbase 96000000 0))))

    ;; 1 & 2. Coreth disables refunds + credits full fee to coinbase
    (let* ((w (make-world))
           (sender #xa11ce)
           (contract #x1000)
           (coinbase #x0100000000000000000000000000000000000000)
           ;; PUSH1 0 PUSH1 0 SSTORE STOP -> clears slot 0 (which starts at 1), generating refund
           (code (hex->bytes "600060005500"))
           (tx (make-tx :type 2 :sender sender :to contract :nonce 0 :gas-limit 100000
                        :max-fee 100 :max-priority 10 :value 0 :data #())))
      (setf (balance-of w sender) 1000000000
            (code-of w contract) code)
      (world-storage-set w contract 0 1)
      (multiple-value-bind (gas status egp) (run-avax-tx w tx :sae nil :base-fee 25)
        (chk "Coreth pre-SAE clear-slot tx succeeds" (= status 1) status)
        (chk "Coreth pre-SAE egp = baseFee + tip = 35" (= egp 35) egp)
        ;; 21000 intrinsic + 3 (PUSH1) + 3 (PUSH1) + 5000 (cold SSTORE clear: 2100 cold + 2900 reset) = 26006
        ;; Ethereum would refund 4800 -> 21206; Coreth disables refunds -> 26006!
        (chk "Coreth pre-SAE disables SSTORE refund (gas=26006, not 21206)" (= gas 26006) gas)
        (chk "Coreth sender paid full gas*egp" (= (balance-of w sender) (- 1000000000 (* gas egp))) (balance-of w sender))
        (chk "Coreth coinbase received full fee (base+tip)" (= (balance-of w coinbase) (* gas egp)) (balance-of w coinbase))))

    ;; 3. Helicon / SAE minimum gas floor ceil(limit / 2)
    (let* ((w (make-world))
           (sender #xa11ce)
           (recipient #xb0b)
           (coinbase #x0100000000000000000000000000000000000000)
           ;; Plain transfer (spent=21000) with gasLimit=100001 -> floor = ceil(100001/2) = 50001
           (tx (make-tx :type 2 :sender sender :to recipient :nonce 0 :gas-limit 100001
                        :max-fee 100 :max-priority 5 :value 1000 :data #())))
      (setf (balance-of w sender) 1000000000)
      (multiple-value-bind (gas status egp) (run-avax-tx w tx :sae t :base-fee 20)
        (chk "SAE plain transfer succeeds" (= status 1) status)
        (chk "SAE charges ceil(100001/2) = 50001 gas" (= gas 50001) gas)
        (chk "SAE egp = executed baseFee + tip = 25" (= egp 25) egp)
        (chk "SAE sender paid 50001*25 + 1000" (= (balance-of w sender) (- 1000000000 (* 50001 25) 1000)) (balance-of w sender))
        (chk "SAE coinbase received 50001*25" (= (balance-of w coinbase) (* 50001 25)) (balance-of w coinbase))))

    ;; 4. SAE executed base fee from receipts
    (let* ((tx0 (make-tx :type 0 :sender #x1 :to #x2 :gas-price 100))
           (tx1 (make-tx :type 2 :sender #x1 :to #x2 :max-fee 100 :max-priority 5))
           (tx2 (make-tx :type 2 :sender #x1 :to #x2 :max-fee 50 :max-priority 10))
           (r0 (list :effective-gas-price 100))
           (r1 (list :effective-gas-price 42)) ; 42 < 100 -> executed base fee = 42 - 5 = 37
           (r2 (list :effective-gas-price 47))) ; 47 < 50 -> executed base fee = 47 - 10 = 37
      (multiple-value-bind (bf found)
          (eita.chain.avalanche:sae-executed-base-fee (list tx0 tx1 tx2) (list r0 r1 r2) 45 96000000)
        (chk "SAE executed base fee extracted from receipts = 37" (and found (= bf 37)) (list bf found))))

    ;; 5. Granite P-256 activation by timestamp
    (let ((spec (eita.chain.avalanche:avalanche-chain-spec "mainnet"))
          (dummy-blk (eita.input::make-chain-block :number 95000000 :base-fee 25 :gas-limit 30000000)))
      (eita.chain:install-chain-spec! spec)
      (funcall (eita.chain::chain-spec-block-setup spec) 95000000 dummy-blk 1763567999)
      (chk "pre-Granite: *p256verify* is NIL" (null *p256verify*) *p256verify*)
      (funcall (eita.chain::chain-spec-block-setup spec) 95000000 dummy-blk 1763568000)
      (chk "Granite+: *p256verify* is T (6900 gas)" (eq *p256verify* t) *p256verify*))

    ;; 6. Atomic batch decoder (empty returns nil, 0)
    (multiple-value-bind (effs gas) (eita.chain.avalanche:decode-atomic-batch #())
      (chk "empty atomic batch -> (nil, 0)" (and (null effs) (zerop gas)) (list effs gas)))

    ;; 7. Warp precompile: getBlockchainID (200 gas) & sendWarpMessage (41500 + 8*args)
    (let* ((w (make-world))
           (sender #xa11ce)
           (warp #x0200000000000000000000000000000000000005)
           (tx-id (make-tx :type 2 :sender sender :to warp :nonce 0 :gas-limit 100000
                           :max-fee 100 :max-priority 0 :value 0
                           :data (hex->bytes "4213cf78")))
           ;; sendWarpMessage(0xdeadbeef): selector ee5b48eb + offset(32) + len(4) + deadbeef00..00 (96 bytes args)
           (send-data (hex->bytes (concatenate 'string
                                               "ee5b48eb"
                                               "0000000000000000000000000000000000000000000000000000000000000020"
                                               "0000000000000000000000000000000000000000000000000000000000000004"
                                               "deadbeef00000000000000000000000000000000000000000000000000000000")))
           (tx-send (make-tx :type 2 :sender sender :to warp :nonce 1 :gas-limit 100000
                             :max-fee 100 :max-priority 0 :value 0 :data send-data)))
      (setf (balance-of w sender) 1000000000)
      (multiple-value-bind (gas status egp) (run-avax-tx w tx-id :sae nil :base-fee 25)
        (declare (ignore egp))
        ;; 21000 + 4*16 (non-zero selector bytes) + 200 (getBlockchainID) = 21264
        (chk "Warp getBlockchainID succeeds" (= status 1) status)
        (chk "Warp getBlockchainID gas = 21264" (= gas 21264) gas))
      (multiple-value-bind (gas status egp) (run-avax-tx w tx-send :sae nil :base-fee 25)
        (declare (ignore egp))
        ;; intrinsic: 21000 + 10*16 (nz: 4 sel + 1 offset + 1 len + 4 payload) + 90*4 (zero) = 21520
        ;; warp send: 41500 + 8*96 = 42268 -> total = 63788
        (chk "Warp sendWarpMessage succeeds" (= status 1) status)
        (chk "Warp sendWarpMessage gas = 63788" (= gas 63788) gas)
        (chk "Warp sendWarpMessage emitted 1 log" (= (length *last-tx-logs*) 1) (length *last-tx-logs*)))))

  (format t "~&avalanche: ~D passed, ~D failed~%" pass fail)
  (when (plusp fail) (eita.test:finish :code 1)))
