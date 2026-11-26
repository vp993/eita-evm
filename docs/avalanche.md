# Avalanche C-Chain in Eita

Eita supports the Avalanche C-Chain (Mainnet `43114` and Fuji `43113`) from **Etna** through **Fortuna**, **Granite**, and **Helicon (ACP-194 SAE)**. Pre-Etna blocks are refused loudly.

## 1. Lean 4 Specifications & Emitted Rules

Every fork threshold, gas formula, SAE floor, Warp price, atomic gas formula, and SAE `GasClock` pricing formula is specified and proved in Lean 4 under [`lean/Evm/Avalanche/`](../lean/Evm/Avalanche/), audited in [`lean/AxiomAudit.lean`](../lean/AxiomAudit.lean) with zero new axioms and zero `sorry`, and emitted into [`src/generated/host.lisp`](../src/generated/host.lisp) by `lake exe emitHostCL`:

| Lean Module | Scope | Key Emitted Functions (`src/generated/host.lisp`) |
| --- | --- | --- |
| `Evm.Avalanche.Forks` | Mainnet & Fuji activation timestamps (Etna, Fortuna, Granite, Helicon), Cancun EL analog (`16`), Granite P-256 activation, `header.extraData` predicate offset (`80 / 24 / 0`) | `spec-avax-mainnet-fork-ord`, `spec-avax-fuji-fork-ord`, `spec-avax-eth-analog-ord`, `spec-avax-p256-active`, `spec-avax-sae-active`, `spec-avax-extradata-offset` |
| `Evm.Avalanche.Charging` | Coreth disabled refunds, full fee to coinbase (`0x0100…0000`), Helicon/SAE `ceil(gasLimit / 2)` floor, wei conservation | `spec-avax-sae-gas-floor`, `spec-avax-tx-gas-used` |
| `Evm.Avalanche.Warp` | Warp Messenger (`0x0200…0005`) gas schedule (Granite `graniteGasConfig`), predicate access-list pricing, `0xff` delimiter | `spec-avax-warp-blockchain-id-gas`, `spec-avax-warp-send-gas`, `spec-avax-warp-read-gas`, `spec-avax-warp-predicate-gas` |
| `Evm.Avalanche.Atomic` | Post-block codec v0 `ImportTx` / `ExportTx` gas schedule and AVAX nAVAX→wei conversion | `spec-avax-atomic-tx-gas` |
| `Evm.Avalanche.GasClock` | ACP-194 SAE `GasClock` (`BeforeBlock`, `BaseFee`, `AfterBlock`, saturating EIP-4844 `fake_exponential`) | `spec-avax-gasclock-target`, `spec-avax-gasclock-min-price`, `spec-avax-gasclock-price-at`, `spec-avax-gasclock-base-fee` |
| `Evm.Avalanche.Trie` | Coreth `NormalizeStateKey` (`slot[0] &= 0xfe` before `keccak256`) and 5-field `multiCoin` account RLP | Specification & idempotence proofs |
| `Evm.Avalanche.Header` | Exact 30-field Coreth `HeaderSerializable.EncodeRLP` layout and SAE `settledHeight ≤ number` invariants | Specification & monotonicity proofs |
| `Evm.Avalanche.Emit` | `Evm.CL.GasEmit.Expr` terms + 17 `*_eq_spec` equivalence proofs | Emitted via `lean/EmitHostCL.lean` |

## 2. Runtime Seams

- **Chain spec (`src/chains/avalanche.lisp`)**: thin seams over `spec-avax-*` (verified by `scripts/check-derived-gas.py` with zero allowlist additions). Supports:
  - Full 4-selector Warp Messenger (`getBlockchainID`, `sendWarpMessage`, `getVerifiedWarpMessage`, `getVerifiedWarpBlockHash`), access-list predicate withholding, and `header.extraData` `predicate.BlockResults` bitset parsing.
  - Coreth post-block atomic `ImportTx` / `ExportTx` state effects and gas accounting.
  - ACP-194 SAE `GasClock` (`gasclock-from-settled`, `gasclock-before-block!`, `gasclock-after-block!`, `gasclock-base-fee`) cross-checked against dynamic-fee receipts.
  - Granite P-256 (`0x100`, EIP-7951 at 6,900 gas) via `*p256verify*`.
- **Coreth 30-field header hashing (`src/input/decode.lisp`)**: every Avalanche header carrying `extDataHash` is RLP-encoded in Coreth's exact `HeaderSerializable.EncodeRLP` order and verified cryptographically against `blk-hash` by `fetch-verified-block`.
- **Coreth Merkle-Patricia Trie (`src/state/mpt.lisp`, `mpt-live.lisp`, `snapshot.lisp`, `snapshot-world.lisp`, `root.lisp`)**:
  - `*coreth-trie*` (enabled automatically by `avalanche-chain-spec`) applies `NormalizeStateKey` (`slot[0] &= 0xfe`) before `keccak256` in live storage roots, incremental MPT, v2 snapshot lookups, and v2 layer writes.
  - Coreth account leaves always encode 5 RLP fields (`[nonce, balance, storageRoot, codeHash, multiCoin]`, with `0x80` for `false` and `0x01` for `true`); v2 account flag bit 3 (`+flag-multi-coin+ = 8`) preserves `multiCoin = true` across imports, layers, compactions, and `snapshot-root`.

## 3. Importing a Full Avalanche Snapshot (NSBFLAT2 → Eita v2)

[`rust/avax-nsb-export`](../rust/avax-nsb-export/) streams a verified Avalanche C-Chain `NSBFLAT2` full snapshot directly into `eita import-reth` in a single pass with ~4 MiB RAM and zero sorting, then cryptographically proves the full state root via `snapshot-root`:

```bash
eita-avax-nsb-export --manifest manifest.json --flat-dir flat/v2/ \
  | bin/eita import-reth --state /path/to/eita-avax-state --threads 16 -
```

## 4. Running Replay

Full-state replay from an imported snapshot:

```bash
bin/eita replay --chain avalanche --state /path/to/eita-avax-state \
  --resume 95999994 --blocks /path/to/blocks --from 95999995 --to 96000099
```

Lazy differential replay against cached block+trace fixtures (pre-SAE, post-SAE, or live tip):

```bash
bin/eita replay --chain avalanche --lazy \
  --source examples/avalanche-poc-source.lisp \
  --observer examples/avalanche-poc-verifier.lisp \
  --blocks data/avalanche-poc/pre-sae --from 95000000 --to 95000024
```
