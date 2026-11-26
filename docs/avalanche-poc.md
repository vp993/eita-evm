# Avalanche C-Chain PoC on Eita

Proof-of-concept adding Avalanche C-Chain (`avalanche` mainnet `43114` and `avalanche-fuji` `43113`) as a `chain-spec` on Eita, with a lazy prestate-backed replay source and a deep differential verifier against an Avalanche archive node.

## What is implemented

- **`src/chains/avalanche.lisp`** (`eita.chain.avalanche`, registered as `"avalanche"` and `"avalanche-fuji"`):
  - **Fork**: Cancun (`*fork-ord* = 16`, TLOAD/TSTORE/MCOPY/BLOBBASEFEE, precompiles `0x01..0x0a`, no EIP-7623 calldata floor, no EIP-7702 delegation).
  - **Granite P-256 (`0x100`)**: enabled via `*p256verify* = t` (EIP-7951 at 6,900 gas) from `1763568000` (mainnet) / `1761750000` (Fuji).
  - **Coreth pre-execution warming**: warms `0x0100…0000`, `0x0100…0001`, `0x0100…0002` (and `0x100` via Eita's precompile seed when Granite is active). Warp (`0x0200…0005`) is left cold unless named in the access list.
  - **Coreth charging & fee routing**: EVM gas refunds disabled (`refund = 0`); full fee (`gasUsed * effectiveGasPrice`) credited to coinbase (`0x0100…0000`), not burned.
  - **Helicon / SAE (ACP-194)** (detected via `settledHeight` in the block header):
    - Executed base fee derived from dynamic-fee receipts (`effectiveGasPrice - maxPriorityFeePerGas`, consistency-checked across the block) and passed into `block-ctx` so both `effectiveGasPrice` and `BASEFEE` see the exact executed fee.
    - Minimum gas floor `charged = max(spent, ceil(gasLimit / 2))`; gas below the floor is withheld from the sender and paid to coinbase.
    - Block gas capacity tracked by sum of tx gas limits; post-block header gate verifies `Σ tx.gasLimit + atomicGas == header.gasUsed`.
  - **Pre-block system calls**: none (no EIP-4788 / EIP-2935 code deployed on C-Chain).
  - **Post-block atomic transactions**: codec v0 `ImportTx` / `ExportTx` decoder (`blockExtraData`) applying AVAX credits, debits, nonce increments, and atomic gas accounting.
  - **Warp precompile (`0x0200…0005`)**: Granite `getBlockchainID()` (200 gas) and `sendWarpMessage(bytes)` (41,500 + 8/arg-byte, SHA-256 message ID, `SendWarpMessage` log). `getVerifiedWarpMessage` / `getVerifiedWarpBlockHash` and native multi-coin precompiles (`0x0100…0001/2`) fail loudly.
- **Minimal core seams**:
  - `src/input/decode.lisp`: decodes optional Avalanche header fields (`extDataGasUsed`, `blockExtraData`, `settledHeight`).
  - `src/input/source.lisp` + `src/chains/chain-spec.lisp`: `skip-eth-header-hash` chain-spec option for non-Ethereum RLP headers (still verifying `parentHash` continuity, `transactionsRoot`, and all tx sender signatures).
- **PoC tooling & tests**:
  - `scripts/fetch-avalanche-poc.py`: fetches blocks, receipts, `prestateTracer` (`diffMode: false` and `diffMode: true`), and 256 ancestor hashes from an archive RPC.
  - `examples/avalanche-poc-source.lisp`: lazy block + prestate source (faults each account/slot once on first touch; keeps `*keep-zero-slots* = t` so state carries continuously across consecutive blocks).
  - `examples/avalanche-poc-verifier.lisp`: observer checking every tx's logs (`address`, `topics`, `data`) against `eth_getBlockReceipts` and every modified account (`balance`, `nonce`, `code`) and storage slot (including deletions to 0 + reverse check for zero extra writes) against `prestateTracer diffMode:true`.
  - `tests/avalanche.lisp`: offline unit tests (Coreth no-refund, coinbase fee routing, SAE `ceil(limit/2)` floor, SAE executed base fee extraction, Granite P-256 activation, Warp `getBlockchainID` + `sendWarpMessage`, atomic decoder).

## Reproducing the verification run

```sh
./build.sh --leanless

# Fetch a window from an Avalanche C-Chain archive node
python3 scripts/fetch-avalanche-poc.py \
  --rpc "$AVAX_RPC_URL" \
  --out data/avalanche-poc/post-sae-100 \
  --from 96000000 --to 96000099

# Replay and verify bit-exact against receipts + prestateTracer diffs
EITA_NO_JIT=1 DYLD_LIBRARY_PATH=/opt/homebrew/lib bin/eita replay \
  --chain avalanche --lazy \
  --blocks data/avalanche-poc/post-sae-100 \
  --source examples/avalanche-poc-source.lisp \
  --observer examples/avalanche-poc-verifier.lisp \
  --from 96000000 --to 96000099
```

## Measured verification results (2026-11-26, Apple Silicon, `--leanless` interpreter-only)

| Window | Blocks | Txs | Total gas | Logs checked | Accounts checked | Slots checked | Gate / diff mismatches | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Pre-SAE (`95,000,000–95,000,010`) | 11 | 60 | 13,316,275 | 80 | 181 | 145 | **0** | Consensus `gasUsed`, `logsBloom`, `receiptsRoot` all verified; includes real `sendWarpMessage` tx (`95,000,008` tx 0) |
| Post-SAE (`96,000,000–96,000,099`) | 100 | 528 | 475,358,200 | 2,314 | 2,448 | 9,687 | **0** | 2.47 s pure replay (~192 MGas/s incl. JSON decode & startup, no JIT); SAE header `gasUsed == Σ limits + atomic` verified |
| Tip (`101,439,000–101,439,024`) | 25 | 56 | 22,672,904 | 172 | 223 | 463 | **0** | Live chain tip on 2026-11-26 |

## Remaining gaps for production full-state Avalanche on Eita

1. **Warp verified reads** (`getVerifiedWarpMessage` / `getVerifiedWarpBlockHash`, e.g. block `95,000,011` tx 0): requires parsing Warp access-list predicate gas and the verification bitset in `header.extraData`.
2. **SAE GasClock**: currently executed base fee comes from dynamic-fee receipts; blocks with only legacy/cap-bound txs need the ACP-194 `GasClock` rolled from `settledGasUnix`/`settledGasNumerator`/`settledExcess`.
3. **Full-state root verification**: Coreth normalizes storage trie keys by clearing bit 0 of byte 0 (`slot[0] &= 0xfe`) before keccak256 (for multi-coin namespace separation), and SAE headers commit `stateRoot` at `settledHeight` rather than `number`.
4. **Avalanche RLP header hashing**: computing Coreth's full RLP header hash (including `extDataHash`, `blockGasCost`, `extDataGasUsed`, `minDelayExcess`, SAE settlement fields) to drop `skip-eth-header-hash`.
5. **Lean formalization + JIT**: porting Avalanche's chain rules to `lean/Evm/Avalanche/` and building with Lean for native JIT throughput.
