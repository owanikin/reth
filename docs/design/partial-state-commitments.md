# Remote Partial-State Commitments

Partial-state BAL advancement no longer reads local full state or execution-result
overlays to fill in untracked storage roots. Replay follows forkchoice-selected headers
and retained or downloaded BALs, resolving missing commitments through `SnapClient`.
See [forkchoice-driven advancement](partial-state-forkchoice.md).

## Resolution

For each distinct untracked account with storage changes, request its exact hashed
account key at the child header's state root. Do not request its storage or bytecode.
Tracked storage roots are computed from retained slots; account-only changes do not
need remote resolution.

The resolver checks account RLP, the exact key, and the inclusion or absence proof.
An empty response without proof means unavailable state, not a deleted account
(except at the known empty trie root). The BAL commitment, agreement between the BAL
and resolved account fields, and the complete resulting state root are still checked
before the transition, journal, and canonical checkpoint are committed atomically.

Transport timeouts, dropped connections, and unavailable roots get up to four attempts
at the same root, with a 20-second request timeout and five seconds between attempts.
Invalid proofs are not retried or replaced with local full-state reads. If resolution
fails, the failed child is not committed; previously verified transitions or completed
rollbacks remain valid.

## Unavailable Commitments

After those bounded attempts, an unproven empty response, timeout, dropped connection,
or unavailable snap capability defers advancement instead of terminating the updater.
The driver preserves the verified head and checkpoint, waits five seconds, then
reconciles against the latest forkchoice head. It retries even without a new forkchoice.
A latest-target channel replaces queued execution notifications.

If the chain changed while waiting, journal rollback and BAL replay follow the new
selected branch. Catch-up also rechecks the selected target after fetching proofs,
before committing a transition. Missing rollback journals still require the existing
snap-resync path.

Invalid proofs, malformed accounts, BAL mismatches, root mismatches, and database errors
remain hard failures. A peer's empty response is never treated as account deletion, and
the peer's different canonical root is never substituted for the requested root.

Recovery waits for availability; it cannot manufacture a proof for a branch no connected
peer serves. Such a node can remain behind indefinitely. This does not add peer branch
discovery, side-branch proof serving, or a durable request queue. A restart recovers from
the existing verified checkpoint and reconciles again. Missing retained BALs are requested
from BAL-capable peers; their unavailability also defers advancement.

## Serving Peer

The full peer serves exact-account proofs from a consistent provider snapshot. Root
lookup is limited to its latest 256 canonical headers, including available in-memory
state. The generated proof is verified against the requested root before sending it.
Missing history, pruned state, and unknown roots return unavailable responses, never
the latest account substituted for an older requested version.

Bulk snap account/storage downloads still use the persisted pivot. General historical
snap serving and trie-node healing are not implemented by this change. In particular,
a superseded branch's root may no longer be available from the serving peer.

## Verification

```sh
cargo test -p reth-downloaders snap::tests --lib
cargo test -p reth-provider partial_snap --lib
cargo test -p reth-node-builder launch::partial_state::tests --lib
cargo check -p reth-node-ethereum
```

The advancement regression starts with empty full account/storage tables and a verified
partial checkpoint. It tests mixed tracked/untracked transitions, unavailable and wrong
root responses without checkpoint changes, retrying advancement, and replacement-branch
replay without execution outcomes. It compares roots with complete reference state and
checks that untracked slots were not stored. Provider tests cover inclusion and absence
proofs at both persisted and in-memory roots.

Paused-time driver tests cover repeated unavailable responses without checkpoint or
journal changes, timer-driven recovery without new forkchoices, coalescing superseded
targets, and rejecting a proof whose target was replaced while the request was in flight.
Invalid proofs and integrity errors are not classified as recoverable.

For a Kurtosis smoke test, rebuild **both** nodes with this branch: the partial node
needs the resolver and the full peer needs exact-root proof serving. Keep partial-state
flags only on the partial node. Check its advancement logs and the full peer's `Served
snap account range` logs: commitment requests have equal `start` and `limit` and nonzero
proof counts for nonempty tries. Compare a logged block hash/root with the full node's
header by hash. `Partial-state advancement deferred` means the checkpoint is safe but
the availability problem has not yet been resolved. A recovery test must show subsequent
successful advancement with a matching canonical root, not merely a live task or rising
RPC block height. Repeated terminal failures are not a passing test. Start with artificial
reorg flags disabled; branch-divergence and peer-outage tests are separate scenarios.

This removes the updater's local full-state resolution dependency. The node still runs
normal execution alongside partial state; the partial updater no longer depends on its
canonical notifications.
It does not yet demonstrate partial-only block execution, state expiry, or disk savings.
