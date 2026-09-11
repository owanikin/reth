# Checkpoint-Backed Transaction Validation

With `--partial-state`, Ethereum pool admission opens the verified partial checkpoint
for the canonical `latest` block using the resolved CLI/TOML/contracts-file filter.
Sender balances, nonces, and required bytecode come from that snapshot. Missing,
incomplete, lagging, or mismatched checkpoints do not fall back to normal full state.
Full-node admission is unchanged when partial mode is disabled.

## Admission Policy

- Ordinary senders need account metadata, which is retained for every account.
- A nonexistent sender is known empty, not unavailable, and cannot fund a transaction.
- After Prague, senders with code require the EIP-7702 delegation designation to be
  readable. Untracked, missing, or corrupt required code yields a provider error,
  not `SignerAccountHasBytecode`. Readable non-delegation code remains invalid.
- Sending to an untracked recipient is allowed when admission checks succeed. Pool
  validation does not execute the recipient or inspect its storage. Admission does
  not guarantee that the partial node can execute or build a block with that transaction.
- Hash-only bytecode reads can use code referenced by another tracked account with
  the same hash. This is available, hash-verified data, not a full-state fallback.
- Stateless failures, insufficient funds, and old nonces retain their existing checks.

Unavailable state produces `TransactionValidationOutcome::Error` and the existing
`PoolErrorKind::Other`. It is not a bad transaction: the network does not penalize the
peer or cache the hash as invalid. The transaction is not admitted or automatically
queued for retry; the caller can resubmit once the checkpoint catches up. This also
applies to admission during startup and re-insertion after a reorg.

## Snapshots and Maintenance

An internal validation batch shares one database snapshot, dropped at batch completion.
The next batch opens a new canonical checkpoint. Public validation entry points cannot
substitute caller-supplied full-state providers or reuse stale partial-state caches.

Maintenance account reloads use the checkpoint at the pool's requested block hash.
Unavailable reloads leave addresses dirty and retry with a short backoff. Reloads that
finish after the partial pool head changes are discarded and retried, rather than
overwriting account metadata with an obsolete snapshot.

The prototype still consumes normal canonical execution notifications for pool updates
and maintains full execution state separately. Existing admitted transactions continue
to use normal pool maintenance; this is not a partial-only payload builder, a persistent
deferred-transaction queue, or a new checkpoint-driven revalidation service.

## Tests

```sh
cargo test -p reth-transaction-pool partial_state_txpool --lib
cargo test -p reth-transaction-pool --lib
cargo check -p reth-node-ethereum
```

The focused tests use actual checkpoint-backed providers with empty full account/storage
tables. They cover transfers to untracked contracts, sender nonce/balance checks, required
delegation code, checkpoint failures, cache refresh across head changes/replacement,
batch snapshot isolation, and resubmission after unavailable state without a bad-transaction
classification. A maintenance test verifies that unsupported partial reads do not fall
back to readable full state.

For a devnet smoke test, rebuild the image, wait for successful partial-state advancement,
and confirm the `Transaction pool initialized` log has `partial_state=true`. Submit a
funded ordinary transfer to the partial node. Check its transaction receipt
on the full node. Use only a devnet key. If submission encounters a lagging-checkpoint
error, wait for advancement and resubmit the same signed transaction. This tests live
admission and propagation, not disk savings or partial-only block execution.
