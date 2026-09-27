# BAL-Compatible Partial-State Bootstrap

The partial-state updater applies BAL transitions, not pre-fork execution. A verified
snapshot at genesis is therefore insufficient when one or more subsequent blocks
were produced before BAL activation. A later BAL-bearing head does not close that gap.

## Local Persisted Pivot (Default)

Without a trusted checkpoint in the configuration, fresh sync, checkpoint recovery,
and resync use the same eligibility rules:

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

## Peer-Backed Trusted Bootstrap

To bootstrap without waiting for this node to execute and persist full state, configure
an operator-trusted checkpoint in `reth.toml`, loaded with `--config <path>`. Partial-state
mode must still be enabled, either in TOML or with `--partial-state`. There are no new
CLI flags. This example uses placeholder hashes; replace every identity with values
from your trusted chain source:

```toml
[partial_state]
enabled = true
contracts = ["0x00000000219ab540356cBB839Cbe05303d7705Fa"]
bal_retention = 256

[partial_state.trusted_checkpoint]
chain_id = 3151908
genesis_hash = "0x1111111111111111111111111111111111111111111111111111111111111111"
block_number = 100
block_hash = "0x2222222222222222222222222222222222222222222222222222222222222222"
state_root = "0x3333333333333333333333333333333333333333333333333333333333333333"
```

The worker checks the chain ID and genesis hash against its chainspec, fetches the
checkpoint header from a peer by hash, and checks its hash, number, and state root.
The seed must already be BAL-active, with a BAL commitment unless it is genesis.
It cannot rely on a locally executed child to establish the pre-BAL boundary.

It then downloads all accounts and only tracked storage/code into the partial-state
tables, preserving untracked account commitments. Completion requires a recomputed
root matching the trusted header. Forkchoice-driven BAL replay continues from there.
Neither header selection nor root verification needs local full-state persistence.

On restart, a complete, same-filter partial checkpoint at or above the seed height
takes precedence. Its header is fetched from a peer and its partial root is checked
again. The seed is a starting point, not a permanent branch constraint. A newer
configured seed can supersede an older saved checkpoint; an incomplete checkpoint
or changed filter requires a fresh download.

Unavailable headers, request timeouts, and an empty initial snap refusal are retried.
Selection and initial availability failures do not reset a saved checkpoint. Once
the first successful account response starts replacement, the checkpoint is marked
`Syncing`; interrupted or root-mismatched downloads cannot be served as complete.
The old snapshot is not retained as a second copy during replacement.

An out-of-retention reorg, pre-BAL replay, or gap exceeding the replay window must not
fall back to local full state. If the configured seed is no newer than the current
partial checkpoint, advancement stops with `requires a newer trusted checkpoint`.
Configure a newer trusted, serviceable seed and restart the worker via a node restart.

### Trust and Availability

- The operator must trust the source of the checkpoint. Peer header/hash checks and
  matching state roots do not establish execution validity or canonicality by themselves.
- A peer must actually serve this root. The current prototype bulk snap server serves
  its persisted pivot, not arbitrary historical roots. A full node's RPC `latest` header
  does not establish snap availability. Choose a serviceable pivot and arrange for its
  data to remain available throughout bootstrap; pivot discovery is not implemented here.
- Normal Reth execution and its full-state storage remain enabled. This makes the
  **partial bootstrap path** independent, not the whole node a partial-only follower.
  Automatic genesis initialization is also unchanged. Use the empty-full-table Rust
  tests below as evidence of the bootstrap path's independence, not devnet disk size.

## Verification

```sh
cargo test -p reth-node-builder -p reth-node-core -p reth-config partial_state --lib
cargo test -p reth-node-builder -p reth-downloaders snap::tests --lib
cargo check -p reth --bin reth --no-default-features
```

Regression tests model activation at timestamp 48 with earlier blocks at 36 and 42.
They cover waiting with a saved genesis checkpoint, selecting a persisted compatible
pivot, timer-driven readiness, checkpoint/filter eligibility, root/header identity,
pre-BAL replay, post-activation missing commitments, and advancement after snapshot
verification.

Trusted-bootstrap tests start without locally persisted headers or full-state tables.
They download and verify a snapshot, apply a child BAL, and resume its saved checkpoint.
They also cover tracked-only retention, empty state, peer outages, corrupt roots,
chain/header mismatches, filter changes, newer seeds, and refusing local-persistence
fallback when a seed is too old for recovery.

For a default-mode devnet test, keep BAL activation after genesis and ensure it produces
pre-BAL blocks. Expect waiting until persistence reaches a compatible pivot, then a
verified snap completion and subsequent BAL advancement. An increasing `eth_blockNumber`
alone is not evidence that the partial checkpoint advanced. The peer must serve the
selected state root and retained BALs must remain available for catch-up.

For trusted mode, mount the TOML config into the partial node and supply a trusted,
BAL-active pivot served by the other node. Expect `Selected peer-backed partial-state
bootstrap`, then snap completion at exactly that hash/root and subsequent advancement.
Restart with the same data and configuration and expect
checkpoint resume, not another snap download. Test unavailable peer data separately,
confirming no completion is reported until the requested root can be verified.

This change does not force persistence, add historical bulk snap serving, or fix consensus
client block-production problems. Do not start an outage/recovery test until both nodes
are following the same chain and partial-state checkpoints are advancing normally.
