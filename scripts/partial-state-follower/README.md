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
failures, outage recovery and cleanup. They are **not** a replacement for the live
commands below. CI runs only this host suite, without a devnet or privileged Docker.

## Producer setup

You can reuse a healthy existing donor running this branch with `--enable-eth71`.
Both donor and follower must use the **same immutable image ID**. Rebuilding a tag
does not update running containers. The full donor must produce BAL-bearing blocks
and retain their BALs; advertising eth/71 or having a BAL hash in a header alone
does not prove BAL serving works.

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

`PASS smoke test` is the completion marker. The follower stays running so outage
testing can use it. On failure/interruption, the script attempts to stop only the
new follower and retains evidence/volume. It does not delete data or prune Docker.
A missed snap-pivot availability window can retry bootstrap up to three times;
root mismatches and explicit sync failures do not become successful retries.

The four offline table checksums cover advancement **before** the planned restart;
the later phase checks the resumed root and execution boundary, not a second
offline checksum. This is normal-stop recovery, not an abrupt-crash test. Full
genesis initialization and shared bytecode storage are not counted as zero storage.

## 3. Isolate the follower's peer connection

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

## Evidence and limits

Only `result.json` with `passed: true` records completion of its scenario.
`failure.json`, partial PASS lines, or a successful Kurtosis launch are not enough.
Each invocation uses a new directory. Evidence includes image/container identities,
checkpoint comparisons, Engine responses and logs; generated data is never staged
by these tools. Root mismatches, invalid responses, malformed JSON, permission
errors and persistent DB inconsistency fail the test. Only the known transient
static-file consistency error is retried, at most five times, without bypassing
Reth's checks.

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

These checks cover a bounded outage within peer retention and a normal restart.
They do not establish deep-reorg recovery, crash consistency, arbitrary historical
state availability, state expiry, disk savings, or execution validity. Scripts
assume a local IPv4 Docker network, a serving peer on TCP/30303, and accessible
`eth`/`admin` RPC plus `reth db` in the image.
