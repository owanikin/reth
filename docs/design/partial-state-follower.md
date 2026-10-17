# Opt-In Partial-State Follower

This experimental, **non-validating, read-only** mode runs the partial-state worker
without launching normal block execution, staged backfill, engine persistence,
the static-file producer, or the pruner. It is not a validating execution client
and must not be used for validator duties or block production.

## Configuration

In the TOML config passed to `reth node --config <path>`, add:

```toml
[partial_state]
enabled = true
follower = true
contracts = ["0x00000000219ab540356cBB839Cbe05303d7705Fa"]
bal_retention = 256
```

Also supply `[partial_state.trusted_checkpoint]` with the chain ID, genesis hash,
block number, block hash, and state root described in
[trusted bootstrap](partial-state-bootstrap.md#peer-backed-trusted-bootstrap).
These must come from a trusted source on your chain, not example hashes. The
checkpoint must be BAL-compatible and available from a snap peer. There is no new
CLI flag for follower mode: `follower` defaults to `false`, so existing
configurations are unchanged.
Partial mode can be enabled in TOML or with `--partial-state`.

### Peer Protocols

Partial-state nodes automatically advertise `eth/71` for BAL requests, alongside
the default eth versions and snap support. A **full serving peer** must use this
build with `--enable-eth71`. It does not need `--partial-state` and should remain a
normal validating node. The flag enables protocol advertisement and retains BALs
from incoming and locally built payloads in the bounded in-memory BAL store. It
does not activate a fork, generate BALs before the chain's BAL activation, or
start the partial-state updater on the full peer. Retained BALs do not survive a
full-peer restart.

For the full Reth participant in Kurtosis, add:

```yaml
el_extra_params:
  - "--enable-eth71"
```

Keep any other flags that participant requires. Rebuild the image and use it for
both the follower and the full peer. On the follower, `admin_peers` should report
`eth/71` and `snap/1` in `caps`, and `protocols.eth.version` must be `71`. Advertised
capabilities alone are not enough: a connection can fall back to `eth/69` if only
one end opted in. Snap bootstrap can succeed on that connection, but subsequent
BAL requests return `UnsupportedCapability`. Ordinary full nodes without the flag
continue to use the default eth versions.

Missing seeds, disabled partial mode, dev mining, execution extensions, and
execution-specific debug sync/reorg controls are rejected. Use a separate full
node for block production. This mode does not delete anything from existing
datadirs; use a separate fresh datadir to measure storage behavior.

## Target and State Processing

- The worker bootstraps from the trusted seed or resumes a compatible saved
  checkpoint. Peer outages do not authorize fallback to local full state.
- Authenticated Engine `forkchoiceUpdated` messages provide the latest target
  hash. Repeated identical targets do not cancel outstanding retries.
- The worker fetches headers, BALs, and missing account commitments from peers,
  applies transitions, and verifies roots before committing each checkpoint.
  Existing rollback journals and retention rules apply.
- `newPayload` can supply BAL bytes to the existing store. It does not execute
  the payload or choose the canonical target. BAL commitments are checked during
  advancement, not accepted on trust merely because bytes were cached.
- Engine payload and forkchoice responses remain `SYNCING`, even when a partial
  root matches. No `latestValidHash` or payload ID is returned. Payload attributes
  never trigger building. Root equality is not a proof of execution validity.

A standard consensus client may remain execution-optimistic or stop advancing
while it receives `SYNCING`. This is a research follower, not a drop-in validating
EL for a validator-bearing Kurtosis participant. Its target source must continue
to supply trusted head hashes without requiring this node to return `VALID`.

## Read Boundaries

Balance, nonce, code, and storage RPC reads use one complete checkpoint snapshot.
`latest` means the latest **verified partial checkpoint**, which may lag the target.
An exact matching checkpoint number or hash also works. A request must be retried
if the checkpoint advances before an exact-block read opens its snapshot.

Missing or incomplete checkpoints, filter mismatches, untracked state, and missing
tracked data return errors. Pending, safe, finalized, other historical states, and
`requireCanonical: true` are unsupported. No read silently falls back to normal
state tables.

`eth_call` and `eth_estimateGas` use the checkpoint's header and state from the same
database snapshot, even when the ordinary executed head is still genesis. The
header's hash and state root must match the complete checkpoint. EVM block context
(including number, timestamp, fees and randomness) is derived from that header.
State and block overrides are local to the simulation; they never update the
checkpoint or persisted storage. Calls to unavailable code or storage fail,
including nested dependencies. A successful call is not execution validation of
the chain.

Header context is retained separately from executed headers. BAL transitions
commit it atomically with the checkpoint; rollback selects the ancestor's context.
`BLOCKHASH` follows the checkpoint header's parent links, never the normal head.
Unknown in-window history returns an explicit unavailable-state error, not zero
or a full-state fallback. A fresh bootstrap may lack pre-pivot ancestor headers;
calls that need that history remain unavailable. Out-of-window `BLOCKHASH` retains
the EVM's normal zero semantics.

Broader simulation (`eth_simulateV1`, access lists, multi-call and tracing) is not
enabled by this path. Transaction admission/submission remains disabled. Existing
ordinary partial mode retains its current RPC and txpool behavior.

Block/header/receipt RPC and `eth_blockNumber` still reflect the ordinary executed
provider, not the partial checkpoint. On a fresh follower they can remain at
genesis. Use `Partial-state snap sync complete`, checkpoint-resume logs, and
`Advanced canonical partial state with BAL` to observe follower progress. Compare
the logged hash/root to that exact block on the full peer, not its moving `latest`.

## Storage Limits

Genesis initialization is unchanged and can populate full genesis account/storage
tables. Existing full state is not cleared when the mode is enabled. After startup,
the follower does not start execution or backfill to populate those tables further.
Tracked bytecode uses the existing shared bytecode table. This is not yet a zero
full-state-genesis implementation, a state-expiry policy, or a disk-savings benchmark.

## Tests

```sh
cargo test -p reth-config -p reth-node-core -p reth-provider -p reth-rpc -p reth-transaction-pool -p reth-node-builder partial_state_follower --lib
cargo test -p reth-node-builder --test follower
cargo test -p reth-node-core eth71 --lib
cargo test -p reth-network bal_selection_uses_negotiated_eth_version --lib
cargo test -p reth-node-builder -p reth-provider partial_state --lib
cargo check -p reth --bin reth --no-default-features
```

Tests cover opt-in validation, snapshot reads and calls independent of the executed
head, blocked full-state fallbacks, rejected unsupported simulation/admission, `SYNCING` responses,
duplicate targets, a real launcher with no full-state writes on Engine requests,
BAL advancement from a verified snapshot with empty full-state tables, and
negotiated BAL exchange between a TOML-configured follower and an opted-in full
server. Full-node coverage builds a payload, imports it into another full node,
and retrieves both nodes' retained BALs over eth/71 without manually seeding the
stores. Tests also cover eth/69 fallback and rejection of BAL requests when only
the remote peer advertised eth/71.
These tests do not replace a devnet run of the newly built image.

The `partial_state_follower_rpc_state_across_reorgs` integration test launches the
follower and queries its HTTP RPC server after equal-height branch replacement,
extension, rewind and replay. A scripted network peer serves headers, BALs and
account proofs for full-state fixtures; the follower must compute the same roots
without changing its normal full-state tables or execution checkpoints. Storage,
call results and block context must follow the selected branch, stale checkpoint
hashes must fail, and untracked state remains explicitly unavailable. These
synthetic branches test commitment/RPC consistency, not consensus or execution
validity. No Docker build is required to run this test.

The maintained [lifecycle test runbook](../../scripts/partial-state-follower/README.md)
provides a small devnet configuration, producer preflight, bootstrap/advancement/
restart smoke test, and an explicitly authorized peer-outage scenario. These host
scripts leave the full producer unchanged and use a separate test follower.
They retain machine-readable evidence separately from the source, including root
comparisons, execution-boundary checks and firewall cleanup results. The host-only
regression suite runs in CI; live Docker scenarios remain opt-in.
Use the smoke script's `--call-data` option to compare calls and estimates against
the full producer at the exact checkpoint hash before and after a normal restart.
Then run `scripts/partial-state-follower/rpc_boundaries.py --run <smoke-evidence>`
to check untracked state reads, direct and nested missing-code errors, known-empty state,
request-local overrides, and unchanged partial/full-state table checksums. It
reuses the running follower without sending targets, deploying contracts, or
restarting nodes; its nested-call fixtures are simulated code overrides. See the
runbook for fixture prerequisites and the exact passing criteria.
