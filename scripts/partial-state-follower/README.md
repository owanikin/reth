# Partial-state follower lifecycle tests

These opt-in host scripts test a **non-validating research follower** against an
already-running full Reth producer. They use Python 3.9+ (standard library only),
a local Docker daemon and the Docker CLI. They do not start or stop Kurtosis,
the donor, its consensus client, or its validators. No private keys are needed.

The follower accepts trusted head hashes supplied by this test. Root equality is
not execution validation. See the [follower design](../../docs/design/partial-state-follower.md).

## Quick check without Docker

From the Reth repository root:

```sh
python3 -m unittest discover -s scripts/partial-state-follower -p 'test_*.py' -v
```

These tests mock Docker and RPC to exercise the actual orchestration, including
failure paths. They test root/canonical checks, honest Engine responses, read-only
DB retries, restart detection, protocol checks, secret permissions, checksum
failures, outage recovery, abrupt-stop authorization and cleanup. They are **not**
a replacement for the live commands below. CI runs only this host suite, without
a devnet or privileged Docker.

## Rust reorg integration test

```sh
cargo test -p reth-node-builder --test follower \
  partial_state_follower_rpc_state_across_reorgs --offline -j 2
```

This launches an isolated follower with HTTP RPC and a local scripted eth/71 +
snap peer. It needs loopback sockets but no Docker, Kurtosis, or internet access.
It bootstraps a verified pivot, switches between equal-height branches, extends,
rewinds, and replays the old branch. At each target it checks storage, calls, gas
estimates, block context (including `BLOCKHASH`), explicit untracked-state errors,
and rejection of stale checkpoint hashes. Roots are computed independently from
the complete fixture state. Full-state table contents and execution stage
checkpoints must stay at their genesis baseline; Engine responses stay `SYNCING`.
The peer serves commitment-consistent synthetic branches, not execution-validated
blocks. This is a deterministic integration test, not a live consensus reorg or
crash-recovery test, and it does not touch existing devnets.

### Restart with an offline reorg

```sh
cargo test -p reth-node-builder --test follower \
  partial_state_follower_restart_ --offline -j 2 -- --test-threads=1
```

These two tests stop the follower on branch A, drop its runtime and database
handles, and reopen the same datadir before selecting branch B. The retained-history
case checks rollback/replay, matching roots, checkpoint-backed storage/calls/gas
estimates, stale-hash errors, continued advancement, and no repeat snap download.
The insufficient-history case advances 65 blocks from the common pivot with the
supported 64-block retention window, allowing the worker to prune its oldest journal.
After restart it checks a worker failure, no acceptance of branch B, and readable
verified state at the last successfully rolled-back ancestor.
Both check durable checkpoints/journals and unchanged normal full-state tables.
This is graceful restart coverage, not an abrupt-crash or live consensus test.
No Docker rebuild or Kurtosis run is needed for these test-only changes.

## Producer setup

Availability metadata adds two tables to the lifecycle checks. Existing completed
partial checkpoints from before this change need a fresh follower volume; do not
reuse them as test runs for this version. See the
[availability design](../../docs/design/partial-state-availability.md).

You can reuse a healthy existing donor running this branch with `--enable-eth71`.
Both donor and follower must use the **same immutable image ID**. Rebuilding a tag
does not update running containers. The full donor must produce BAL-bearing blocks
and retain their BALs; advertising eth/71 or having a BAL hash in a header alone
does not prove BAL serving works.

For RPC boundary tests, the full producer also needs
`--rpc.eth-proof-window=4096`, as set in the checked-in YAML. Reth defaults to a
zero-block historical proof window. The fixture checks use `eth_getProof` at the
follower's fixed checkpoint, which is usually behind the producer's head. This is
a bounded local-test setting, not a recommendation for public RPC servers.

For a fresh, small network, `network_params.yaml` configures one full producer,
Lighthouse, and a separate validator client. The test follower is created later,
without its own consensus client or validators. The supernode setting permits the
single-participant PeerDAS setup; 24-second slots leave more time on slower hosts.

The local ethereum-package reference checkout used during development is
`ba855e0ada309474aa5df4f438a7b287dfc19780`. Use a separate checkout so existing
devnet configuration is not overwritten:

```sh
git clone https://github.com/ethpandaops/ethereum-package.git ../ethereum-package-follower
git -C ../ethereum-package-follower checkout --detach ba855e0ada309474aa5df4f438a7b287dfc19780

# Only needed when the Reth binary changed; host-script edits need no rebuild.
docker build --progress=plain --build-arg BUILD_PROFILE=release \
  -t reth-partial-statefulness:local .

kurtosis run --enclave reth-ps-lifecycle ../ethereum-package-follower \
  --args-file scripts/partial-state-follower/network_params.yaml
```

The checked-in YAML names the development Lighthouse and genesis-generator tags.
These are experimental fork-specific dependencies, not guaranteed immutable
releases. For an archived run, pin their `image@sha256:...` digests in your args
file and retain that file plus the package commit. Retrieve installed digests with
`docker image inspect --format '{{json .RepoDigests}}' IMAGE`. The helpers record
the resolved Reth and network-helper image IDs, not just their tags.

Use `kurtosis service inspect reth-ps-lifecycle el-1-reth-lighthouse` and
`docker ps` to identify the donor. Pass its exact container name or ID as `DONOR`;
do not reuse ports or IDs from another enclave. Multiple matches are not accepted.
On macOS, prefix long-running commands with `caffeinate -dimsu` to prevent sleep.

## 1. Check the producer

```sh
export DONOR='<full-producer-container-name-or-id>'

python3 scripts/partial-state-follower/preflight.py --donor "$DONOR"
```

Wait for `PASS producer preflight`. This requires five increasing head samples,
a BAL-bearing header and progress in the persisted `Finish` checkpoint. It can
wait up to 15 minutes. A stationary chain is not a follower failure: investigate
the donor/consensus logs before testing. Preflight does not yet test BAL delivery.

Default donor paths are `/data/reth/execution-data` and
`/network-configs/genesis.json`. Both preflight and smoke accept
`--donor-datadir` and `--donor-chain` for other layouts.

## 2. Bootstrap, advance and restart a follower

```sh
python3 scripts/partial-state-follower/smoke.py \
  --donor "$DONOR" --image reth-partial-statefulness:local --timeout 240
```

The script creates a unique follower container and volume. It publishes RPC only
on loopback, discovers assigned ports, and allows only the donor as a trusted peer.
No fixed host ports or developer-specific paths are embedded. Optional flags:

- `--output-dir PATH`: evidence root (default: this repo's ignored `target/partial-state-follower/`).
- `--tracked-contract ADDRESS`: retain and compare a contract with nonempty code
  (default: the devnet deposit contract).
- `--timeout SECONDS`: progress budget per phase, not the whole run.
- `--call-data 0x...`: compare `eth_call` and `eth_estimateGas` against the donor at
  the exact checkpoint hash, before and after restarting the follower. Supply
  calldata for a read-only method of the tracked contract whose dependencies are
  retained. This requires an image with checkpoint-backed follower calls; it is
  opt-in so the original lifecycle test can still run against older images.

The live checks are:

1. Bootstrap at a fresh persisted BAL-compatible pivot and compare the complete
   checkpoint's number, hash and root to the donor's canonical header.
2. Negotiate eth/71 and snap/1, then reach two newer targets with matching roots.
   Every Engine response must remain `SYNCING`, with no valid hash or payload ID.
3. Compare tracked code to the same checkpoint block on the donor. Require
   `Finish=0` and `eth_blockNumber=0x0` on the follower.
4. Stop **only the test follower** and compare offline checksums of
   `PlainAccountState`, `PlainStorageState`, `HashedAccounts`, and `HashedStorages`
   with their post-genesis baseline.
5. Restart that follower, discover its new ports, require checkpoint resume
   without another snap completion, then advance again and check the root and
   execution boundary. Confirm the donor did not restart.

With `--call-data`, require `PASS before-restart calls`, `PASS after-restart calls` and
`checkpoint_calls_checked: true` in `result.json`. The script retains the paired
call/estimate responses in `before-restart-calls.json` and `after-restart-calls.json`.
The follower checkpoint must remain unchanged during each comparison. A mismatch
or RPC error fails the run; a root match alone is not a passing call test.

For the default deposit contract, `get_deposit_count()` uses calldata `0x621fd130`:

```sh
python3 scripts/partial-state-follower/smoke.py \
  --donor "$DONOR" --image reth-partial-statefulness:local --timeout 240 \
  --call-data 0x621fd130
```

`PASS smoke test` is the completion marker. The follower stays running so RPC boundary
and outage testing can use it. On failure/interruption, the script attempts to stop only the
new follower and retains evidence/volume. It does not delete data or prune Docker.
After each start, it waits for fresh HTTP/Engine port mappings and read-only RPC
readiness probes, bounded by `--timeout`. Connection readiness can retry; exits,
unexpected restarts, authentication failures and RPC errors fail the run. The
`*-rpc-ready.json` files record the ports and attempts used. A failed smoke run
must not be passed to `rpc_boundaries.py`, even if earlier phases passed.
A missed snap-pivot availability window can retry bootstrap up to three times;
root mismatches and explicit sync failures do not become successful retries.

The four offline table checksums cover advancement **before** the planned restart;
the later phase checks the resumed root and execution boundary, not a second
offline checksum. This is normal-stop recovery, not an abrupt-crash test. Full
genesis initialization and shared bytecode storage are not counted as zero storage.

## 3. Verify missing-state RPC boundaries

Use a successful `smoke.py --call-data ...` evidence directory while its follower
and full producer are still running. No image rebuild is needed for host-script
changes. The image must already support checkpoint-backed calls.

```sh
export RUN='<absolute-smoke-evidence-directory>'
python3 scripts/partial-state-follower/rpc_boundaries.py --run "$RUN"
```

The script discovers current ports and checks the follower's **current** checkpoint
against the producer's canonical header. It sends no Engine targets, transactions,
deployments or firewall commands, and never stops or restarts either node. Run it
alone: concurrent target updates or reorgs invalidate a stationary-checkpoint test.
If the producer no longer serves that checkpoint's historical state, create a fresh
smoke follower; do not replace exact-block queries with `latest`.

If `fixture-storage` reports `distance to target block exceeds maximum proof window`,
the producer rejected a fixture prerequisite, not a missing-state check. Start a
producer with `--rpc.eth-proof-window=4096`, then run smoke and boundary tests
against that producer. A new image build is not needed, but editing the YAML does
not change a running container's arguments. Preserve an existing test run by using
a new enclave name. With the flag already set, use a fresh smoke checkpoint if the
old one has fallen more than 4096 blocks behind. The flag permits historical
proofs; it does not restore pruned state. Proof errors still fail the test and
are never accepted as expected missing-state responses.

The default untracked fixture is the devnet beacon-roots contract at
`0x000F3df6D732807Ef1319fB7B8bB8522d0Beac02`, storage slot zero. Use
`--untracked-contract ADDRESS --untracked-slot 0x...` for another contract. The
producer must have nonempty code and a nonempty storage trie there. A separate
`--empty-address` fixture must have empty code and storage, outside the reserved
low-address range. Invalid or unavailable fixtures fail rather than skip checks.

Checks cover:

- `eth_getCode` and `eth_getStorageAt` reject untracked state with `-32002` and
  `-32001` respectively, naming the untracked address.
- Both `eth_call` and `eth_estimateGas` reject direct untracked code access and
  nested access from a tracked caller. Supplying code for an untracked contract
  still returns `-32002`: the account guard rejects it before applying overrides.
- Request-local code overrides implement small SLOAD, SSTORE and CALL fixtures.
  Nested callers write a tracked slot first and discard the CALL success flag;
  a missing dependency must still surface as a database error, not a revert,
  successful empty result or zero gas estimate. The full producer executes the
  same overridden requests as controls. These are simulated fixtures, not deployed
  contracts. The live test does not reach an untracked SLOAD through that guard;
  Rust tests separately use a code-empty account with a nonempty storage commitment
  to exercise `-32001` during direct and nested EVM storage reads.
- Successful SSTORE and `stateDiff` overlays return their simulated values but
  leave normal tracked reads/calls unchanged. Known-empty state returns empty/zero
  results, not missing-state errors. Calls and estimates still match the producer
  after the negative cases.
- Read-only checksums of all eight partial-state tables, the shared bytecode table,
  the four full-state tables and `StageCheckpoints` remain unchanged. The complete
  checkpoint must stay fixed, `Finish` and the executed head stay at genesis, and
  neither node may restart. No write access or consistency-check bypass is used.

Require `PASS RPC boundaries` and `passed: true` in the new
`rpc-boundaries-*/result.json`. `rpc-observations.json` records exact requests and
responses, including expected errors and producer controls. Before/after table
checksums and checkpoint/header comparisons are retained too. Transport failures,
malformed responses, wrong errors, unexpected successes, changed data, and missing
producer state fail the run and leave both nodes untouched.

This proves the tested RPC boundaries, not arbitrary contract support, execution
validity, recovery from missing tracked data, or all behavior during concurrent
checkpoint changes. The Rust RPC tests separately exercise unavailable tracked
bytecode and pinned snapshots across checkpoint changes.

## 4. Isolate the follower's peer connection

Run this soon after smoke, while required peer history remains available. `RUN`
is the exact `Evidence:` directory printed by the maintained `smoke.py`, not a
directory from the old ad hoc scripts.

```sh
export RUN='<absolute-smoke-evidence-directory>'
docker pull nicolaka/netshoot:latest

python3 scripts/partial-state-follower/outage.py --run "$RUN" \
  --timeout 240 --allow-network-disruption
```

Outage requires a successful smoke result and a container labeled as a test
follower. It catches up first and verifies that the donor is its only eth/71 + snap
peer. It never restarts either node. Its temporary helper shares **only the
follower's network namespace** and uses `NET_ADMIN` to drop TCP/30303 traffic to
that peer. Engine/RPC and the producer's own network are left untouched.

The script waits for a new block after isolation and sends exactly one new target.
During at least 65 seconds of observation, the entire checkpoint must remain
unchanged while the producer keeps advancing. It then removes the firewall,
requires nonzero dropped-packet counters, and checks recovery of the **same target
without another forkchoice update**. It sends a further target only after that
check passes. Both roots must match, neither node may restart, and `Finish` must
remain zero. A stale/abandoned target or a stalled producer is not a pass.

The helper image must already be cached; `--netshoot-image IMAGE` accepts a pinned
digest or ID. No automatic pull occurs inside the outage. A separate helper timer
attempts restoration after 240 seconds even if Python exits unexpectedly, provided
Docker keeps running. Normal error/interrupt paths also request cleanup. Do not
kill or pause the helper or Docker during the experiment.

Completion: `PASS controlled outage`. The helper container, firewall counters,
checkpoint samples and recovery comparisons are retained. If cleanup is not
confirmed, use the `helper` name in the outage's `run.json`:

```sh
docker stop --time 40 '<outage-helper-name>'
docker logs '<outage-helper-name>'
```

Require `RESTORED=...`, not `RESTORE FAILED`. If the helper already exited without
restoration, inspect the named `chain` in the follower namespace before further
testing; do not clear unrelated firewall rules or stop the producer.

## 5. Recover after abrupt termination

Use a recent successful `smoke.py --call-data` run. No image rebuild is needed for
this host-script change. Keep its full producer, Lighthouse and validator running;
do not send concurrent Engine targets or run outage/RPC-boundary scripts against
the same follower during this test. Only the labeled smoke-test follower will be
killed and restarted. Its container, config and data volume must still match the
smoke evidence; automatic restart and auto-remove must be disabled.

Set `RUN` to the **actual** `Evidence:` directory printed by that smoke run:

```sh
export RUN='<absolute-smoke-evidence-directory>'
python3 scripts/partial-state-follower/abrupt_restart.py \
  --run "$RUN" --timeout 240 --allow-abrupt-stop
```

On macOS, prefix the `python3` command with `caffeinate -dimsu`. Without
`--allow-abrupt-stop`, the runner refuses to access Docker. It never deletes or
replaces data, restarts the producer, or changes firewall rules.

The scenario:

1. Catch up to a recent canonical target, verify its checkpoint root, and compare
   tracked code, slot zero, `eth_call` and `eth_estimateGas` with the producer at
   that exact block hash. Require `Finish=0` and `eth_blockNumber=0x0`.
2. Record all 16 table checksums used by the RPC-boundary test, including partial
   state, headers, checkpoint, journals, changesets and shared bytecode. The four
   full-state tables must still match smoke's post-genesis checksums.
3. Send `docker kill --signal SIGKILL` to the follower's exact container ID.
   Confirm it stopped with exit code 137, without an OOM kill or automatic restart.
4. Start that same container and discover its fresh HTTP/Engine ports. Require a
   checkpoint-resume log from **after** the kill, not a previous startup, and no
   repeat snap bootstrap. The complete checkpoint, RPC results and all 16 table
   checksums must match the pre-kill observations.
5. Send a newer producer target, require `SYNCING` without a valid hash/payload ID,
   and verify advancement and RPC results against that block. Full-state checksums
   must remain at their genesis baseline, normal execution must remain at zero,
   and the producer must not have restarted or stalled.

Require `PASS abrupt restart` and `passed: true` in the new
`abrupt-restart-*/result.json`. Checkpoint/header comparisons, RPC results,
checksums, container states (including termination evidence), and logs are retained.
The recovered follower stays running. A producer reorg that invalidates the selected
checkpoint fails this scenario; it is not silently accepted as crash recovery.

Failures retain `failure.json` and never become a pass just because cleanup worked.
If interrupted between killing and restarting, the runner attempts one start of
the same follower. A failed start is not retried in a loop. Inspect `failure.json`,
`cleanup.json` when present, `follower-final.json` and `follower.log` before manually
starting that test container. If the Python process itself is killed or Docker
becomes unavailable, cleanup cannot be guaranteed. The producer is never stopped
and the follower volume is never removed.

This deliberately kills an idle process **after a verified committed checkpoint**.
It does not inject a fault inside an MDBX commit, kill the Docker VM, simulate
power loss, or prove every possible crash interleaving. It also does not exercise
rollback after the kill; the Rust restart/reorg tests separately cover journal
replay after orderly shutdown. Passing host-only mocks is not evidence of live
SIGKILL recovery: run this opt-in scenario to obtain that evidence.

## Evidence and limits

Only `result.json` with `passed: true` records completion of its scenario.
`failure.json`, partial PASS lines, or a successful Kurtosis launch are not enough.
Each invocation uses a new directory. Evidence includes image/container identities,
checkpoint comparisons, Engine responses and logs; generated data is never staged
by these tools. Root mismatches, invalid responses, malformed JSON, permission
errors and persistent DB inconsistency fail the test. Live read-only DB queries
retry only the exact errors `File is in an inconsistent state.` and
`cannot get a writer on a read-only environment.` The consistency checker can
report either while static files and MDBX are being committed. The query is retried
at most five times, without enabling writes or bypassing Reth's checks; a persistent
failure never supplies a checkpoint.

`*-comparison.json` records the checkpoint and both producer lookups (by hash and
canonical block number), including unsuccessful comparisons. The error distinguishes
an unavailable header from a different number, hash or state root. The lookups are
separate RPC observations: a producer reorg during catch-up can invalidate the
target before the firewall is installed. Such a run remains failed, even if that
block later becomes canonical again; inspect the saved responses and producer logs
before retrying. These checks do not retry or accept mismatches automatically.

**Do not publish the whole evidence directory.** It contains the generated Engine
JWT in `jwt.hex` (mode 0600). Publish selected result/comparison JSON and reviewed
logs instead. `target/` may be removed by `cargo clean`; archive selected evidence
elsewhere when preparing a report. Do not force-add generated evidence to Git.

These checks cover a bounded outage within peer retention, a normal restart, and
an opt-in SIGKILL/reopen scenario after a committed checkpoint. They do not
establish deep-reorg recovery, arbitrary mid-commit crash consistency, power-loss
durability, arbitrary historical state availability, state expiry, disk savings,
or execution validity. Scripts assume a local IPv4 Docker network, a serving peer
on TCP/30303, and accessible
`eth`/`admin` RPC plus `reth db` in the image.
