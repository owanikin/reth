# Partial-state availability

This is storage groundwork for expiry, not an expiry policy. It introduces no
deletion command, timer, on-demand reacquisition, or shared-bytecode garbage collection.

`PartialStateAvailability` records whether each account's complete storage and
bytecode are locally available. The configured filter remains the retention policy;
it is not evidence that data is present. Known-empty state comes from the account's
storage-root/code-hash commitments. Missing availability metadata is an error, not
an empty slot, available code, or permission to fall back to full-state tables.

Snap completion verifies the state root using downloaded tracked storage, verifies
tracked bytecode hashes, and commits availability with the complete checkpoint in
one database transaction. Incomplete downloads do not acquire availability records.
Repeating completion of an already complete checkpoint does not restore availability.

Checkpoint readers capture availability in the same database snapshot as accounts
and storage. Unavailable tracked storage/code returns an explicit error even if
stale rows or shared bytecode remain locally. Untracked reads still obey the filter.
Hash-only code access is limited to hashes referenced by available tracked accounts.
The partial RPC EVM adapter checks code availability by address before reusing code
cached by hash, so a shared hash cannot bypass an unavailable account's boundary.

BAL advancement uses local slots only for available tracked storage (or a known-empty
parent trie). Otherwise it resolves the post-state commitment from a peer and keeps
storage unavailable. A full bytecode replacement in a BAL can restore tracked code
availability. Root computation preserves unavailable storage commitments. Account
creation/deletion and checkpoint updates include availability atomically.

`PartialStateAvailabilityChangeSets` journals availability alongside existing account
and slot journals, without changing their encoded format. Reverts restore it;
journal pruning and snap reset remove the associated metadata. Restart/reorg and host
lifecycle checks include both new tables.

## Existing prototype data

There is no implicit migration of completed checkpoints or old journals. A completed
checkpoint without these records cannot establish availability and requires a fresh
partial-state bootstrap. Use a new follower datadir/volume for this version; preserve
old test evidence and data. Do not edit the checkpoint or mark metadata available by
hand. Full-node state tables and their encoding are unchanged.

## Verification

```sh
cargo test -p reth-provider partial_ --lib --offline -j 2
cargo test -p reth-node-builder partial_availability --lib --offline -j 2
cargo test -p reth-node-builder --test follower --offline -j 2 -- --test-threads=1
python3 -m unittest discover -s scripts/partial-state-follower -p 'test_*.py'
```

Provider tests inject unavailable metadata without deleting data to exercise the
boundary. Actual expiry must additionally define atomic deletion, retention policy,
reorg interactions with expired data, and verified reacquisition before making data
available again. Journal retention alone is not state expiry.
