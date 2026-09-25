# Forkchoice-Driven Partial-State Advancement

After bootstrap, the partial-state worker follows `forkchoiceUpdated` head hashes directly.
It starts with the node and does not subscribe to executed canonical-chain notifications.
A watch channel keeps only the latest selected head; repeated identical forkchoices do
not interrupt retries or replay.

## Data Path

1. Resume a verified checkpoint or download a BAL-compatible persisted pivot as before.
2. Resolve the selected head and its ancestry by hash. Local headers are an optional cache;
   missing headers are downloaded and their hashes and parent numbers checked.
3. Roll back journaled partial transitions to the common ancestor, then replay forward.
4. Read each BAL retained from `newPayload` or fetch it from a BAL-capable peer. Decode it
   and check its hash against the corresponding header before use.
5. Obtain proof-verified account commitments from snap peers. Apply each transition only
   if the computed partial root matches the header, committing state, journal, and checkpoint
   atomically. No local full-state or execution-result fallback is used.

Forkchoice changes cancel pending work. Replay checks the selected target again after
network awaits before committing a transition. Missing headers, BALs, or peer commitments
defer advancement and retry after five seconds, without needing a new forkchoice or node
restart. Invalid proofs, header/BAL hash mismatches, and root mismatches remain errors.
Worker failure is reported even if the executed chain stops producing notifications.

## Boundaries

- This follows a CL-selected branch; matching BAL and state commitments does **not** prove
  execution validity. The normal engine retains responsibility for Engine API responses.
- Bootstrap, including restart eligibility checks, still uses local header/persisted-state
  providers. Out-of-retention reorg recovery still needs a BAL-compatible persisted pivot.
  Normal execution and full-state storage have not been disabled.
- BAL download requires an `eth/71`-capable peer when the payload BAL is not retained locally.
  Snap proofs require `snap/1` and availability of the requested historical root.
- Ancestry buffering and its verified-header cache are each limited to 4096 headers.
  The cache survives target changes to avoid restarting slow ancestry downloads. Larger
  gaps wait for a newer persisted pivot rather than allocating an unbounded branch.
  Journal retention still bounds rollback.
- A header available only in an unexecuted payload is not yet a local header cache entry;
  a peer must serve it until normal execution makes it locally available.

## Verification

```sh
cargo test -p reth-node-builder launch::partial_state --lib --offline
cargo check -p reth --bin reth --no-default-features --offline
```

The forkchoice tests leave the executed provider at genesis and its full-state tables empty.
They exercise remote headers and BALs, unavailable data and recovery without another
forkchoice, superseded proofs, reorg rollback/replay, and retention limits. These tests are
the direct evidence of independence; increasing `eth_blockNumber` on a normal devnet is not.

After rebuilding the image, repeat the same-image peer-outage test on a healthy devnet.
Record the last verified checkpoint, the deferred-advancement log, and the first resumed
`Advanced canonical partial state with BAL` entry without restarting the partial node.
Compare that entry's hash and root with the full node's header for the **same block hash**.
Keep Engine API/execution progress distinct from partial-checkpoint progress in the report.
