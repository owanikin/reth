# BAL-Compatible Partial-State Bootstrap

The partial-state updater applies BAL transitions, not pre-fork execution. A verified
snapshot at genesis is therefore insufficient when one or more subsequent blocks
were produced before BAL activation. A later BAL-bearing head does not close that gap.

## Pivot Selection

Fresh sync, checkpoint recovery, and resync use the same eligibility rules:

- The pivot's block number, hash, and state root must match its header.
- A pivot at or after Amsterdam activation can be followed by BAL transitions.
  Non-genesis headers at or after activation must contain a BAL commitment.
- A pre-activation pivot is usable only if its immediate canonical child extends
  that pivot and is both BAL-active and BAL-bearing. This permits the last pre-fork
  state, including genesis when the first block is already post-fork.
- A new snapshot is selected from persisted state, not an in-memory head or wall-clock
  time. An orphaned persisted pivot is not used for a new download.

A complete checkpoint with the same filter resumes only when it satisfies these rules.
Otherwise the worker waits for a compatible persisted pivot. It leaves the checkpoint,
partial tables, and journals untouched while waiting, and polls every five seconds even
if no new canonical notification arrives. Incomplete or differently filtered checkpoints
still require a new download.

The waiting log is:

```text
Waiting for a persisted BAL-compatible partial-state pivot
```

Once a usable pivot is available, the existing snap path downloads its state and verifies
the computed root before marking the replacement checkpoint complete. Canonical catch-up
then resumes from that checkpoint using the latest forkchoice head, not queued execution
notifications. See [forkchoice-driven advancement](partial-state-forkchoice.md).

If replay or a reorg reaches a legitimate pre-BAL block, it requests this same bootstrap
path without fabricating an empty BAL or advancing the checkpoint over the block. A
missing BAL commitment after activation remains an error, as do invalid proofs, root
mismatches, and database errors.

## Verification

```sh
cargo test -p reth-node-builder bootstrap --lib
cargo test -p reth-node-builder launch::partial_state::tests --lib
cargo check -p reth-node-ethereum
```

Regression tests model activation at timestamp 48 with earlier blocks at 36 and 42.
They cover waiting with a saved genesis checkpoint, selecting a persisted compatible
pivot, timer-driven readiness, checkpoint/filter eligibility, root/header identity,
pre-BAL replay, post-activation missing commitments, and advancement after snapshot
verification.

For a devnet test, keep BAL activation after genesis and ensure it actually produces
pre-BAL blocks. Expect waiting until persistence reaches a compatible pivot, then a
verified snap completion and subsequent BAL advancement. An increasing `eth_blockNumber`
alone is not evidence that the partial checkpoint advanced. The peer must serve the
selected state root and retained BALs must remain available for catch-up.

This change does not force persistence, add historical bulk snap serving, or fix consensus
client block-production problems. Do not start an outage/recovery test until both nodes
are following the same chain and partial-state checkpoints are advancing normally.
