# Checkpoint-Backed Partial-State RPC

With `--partial-state`, the shared `eth` state loader uses the completed partial-state
checkpoint instead of Reth's normal state tables. This covers balance, nonce, code,
storage, `eth_call`, and `eth_estimateGas`. The normal full-node path is unchanged.

## Behavior

- Account metadata is readable for all accounts. A nonexistent account or an empty
  storage/code commitment is known empty, even if its address is untracked.
- Nonempty untracked storage returns `-32001`; nonempty untracked code returns `-32002`.
- Missing/incomplete checkpoints, filter mismatches, missing tracked bytecode, and
  unsupported partial-state operations return `-32003` with an explanatory message.
- `latest` means the canonical head, not the last available partial checkpoint. If
  partial advancement lags, RPC returns an error until it catches up. Explicit block
  numbers/hashes succeed only when they identify the current complete checkpoint.
- Calls and estimates use the checkpoint's state and captured recent block hashes.
  They fail on unavailable dependencies, including nested calls to untracked code.
  The execution wrapper conservatively rejects loading an untracked contract with
  nonempty code, including EVM operations that only need that account's metadata.
- Pending state, historical snapshots other than the current checkpoint, trie proofs,
  witnesses, and root computation with execution overlays are not supported. Preserved
  account/storage commitments are not substituted for proofs.

This is RPC integration, not removal of Reth's normal execution state. The prototype
still maintains that state separately. Txpool integration and a partial-only execution
path remain separate milestones. This change does not claim coverage of every custom
`debug`, `trace`, or `reth` RPC endpoint.

## Rust Tests

Run from the Reth repository; Docker and Kurtosis are not required:

```sh
cargo test -p reth-rpc partial_state_rpc --lib
cargo test -p reth-provider partial_state_reader --lib
cargo test -p reth-rpc-eth-types partial_state_unavailable_errors_use_stable_codes --lib
```

The RPC tests issue JSON-RPC requests against the actual `EthApi` module backed by a
temporary database. They exercise calls and gas estimation with empty full-state
tables, checkpoint failures, nested unavailable code, and normal full-node reads.

## Devnet Smoke Test

After these pass, rebuild the local image and launch your existing two-node Kurtosis
configuration. Use the new run's HTTP RPC ports from `kurtosis service inspect`, not
ports from an earlier enclave. Set `RPC1` to the partial node and `RPC2` to the full
node. Wait for snap completion and successful partial-state advancement in the logs.

For the tracked deposit contract, run the same request against each endpoint:

```sh
curl -sS "$RPC1" -H 'content-type: application/json' --data \
'{"jsonrpc":"2.0","id":1,"method":"eth_getCode","params":["0x00000000219ab540356cBB839Cbe05303d7705Fa","latest"]}'
```

The returned code should agree. A `-32003` pivot mismatch while the updater is behind
is an availability error, not permission to fall back to full state; retry after
advancement. To compare state that changes per block, query both nodes at the same
block hash from a recent `Advanced canonical partial state with BAL` log. The partial
node can reject this hash if it has already advanced beyond it.

Query a contract you know exists on this devnet but did not configure as tracked:
`eth_getCode` should return `-32002`; `eth_getStorageAt` should return `-32001` if
its storage commitment is nonempty. A random nonexistent address is not a valid
missing-state test because it is legitimately empty.

For call/estimate testing, use a tracked contract's read-only method and a separate
tracked method that calls an untracked contract. The first should succeed when all
dependencies are retained; the latter must return an unavailable-state error, not
an empty result or a successful gas estimate. The Rust fixtures supply both cases
without requiring devnet deployments. RPC success alone does not prove disk savings;
that requires separate database measurements.
