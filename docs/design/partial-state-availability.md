# Partial-state availability

Availability metadata is the groundwork for explicit storage expiry. There is no
automatic expiry policy, timer, on-demand reacquisition, or
shared-bytecode garbage collection.

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
lifecycle checks include both new tables. Explicit expiry also updates old journal
availability so a later revert cannot re-enable deleted storage.

## Explicit storage expiry

`ProviderFactory::expire_partial_state_storage(expected_pivot, filter, addresses)`
expires the selected tracked contracts' storage. It returns counts of accounts
newly made unavailable, current slots deleted, and journal slots deleted.
The caller explicitly chooses the addresses; age-based selection is not implemented.
The offline `reth db partial-state-expire` command exposes this operation for
stopped followers. There is no expiry RPC method.

The operation takes one write transaction, requires the expected complete checkpoint
and filter identity, and verifies the partial root before and after deletion. It
removes current slots and all retained storage before-images for the selected
addresses. Their historical availability is marked unavailable wherever the old
storage commitment was non-empty. Account/root before-images and transition journals
remain, so reorg rollback still restores the correct commitments without restoring
expired slots. Other contracts, bytecodes, headers, normal full-state tables, and
the checkpoint itself are untouched. Any error aborts the entire batch.

New readers receive `StorageUnavailable` for non-empty expired storage, including
previously absent slots: without the complete trie, zero cannot be inferred.
Known-empty or absent accounts still read as zero. Repeating expiry is idempotent,
and duplicate addresses are processed once. Untracked addresses are rejected.
Expiry can also remove old storage history for an account that is empty or absent
at the current checkpoint.

BAL updates to expired non-empty storage continue using peer-resolved account
commitments; a BAL's touched slots alone cannot restore complete storage. If a
verified transition makes the storage trie empty, later BAL updates can reconstruct
complete storage from that known-empty base. Expiry is not a permanent blacklist
or a change to the configured tracking policy. General verified reacquisition is
still separate work.

### Offline operator command

Stop the follower before previewing or applying expiry. Both modes require exclusive
MDBX access, including against processes in other container PID namespaces. Keep the
full producer running. Use the same binary/image, datadir, chain, and complete
follower configuration used to launch the follower; do not point this command at the
producer's database. A backup is recommended: general storage reacquisition is not
implemented.

```sh
reth --quiet db --datadir "$DATADIR" --chain "$CHAIN" --config "$CONFIG" \
  partial-state-expire --address "$ADDRESS"
```

Preview opens MDBX read-only and prints JSON with `applied: false`, the checkpoint,
filter hash, deduplicated addresses, and the proposed counts. No state rows change.
Copy the returned `block_hash` into `CHECKPOINT_HASH`, then explicitly apply:

```sh
reth --quiet db --datadir "$DATADIR" --chain "$CHAIN" --config "$CONFIG" \
  partial-state-expire --address "$ADDRESS" \
  --apply --expected-block-hash "$CHECKPOINT_HASH"
```

`--address` is repeatable or comma-separated. The command requires `enabled = true`
and `follower = true` in the config and reads the full filter from that config (and
its optional contracts file). CLI-only launch overrides are not inferred; use an
explicit configuration matching the checkpoint filter. Relative contracts-file
paths resolve from the working directory, as on node launch.

Missing/malformed config, wrong chain/genesis, advanced normal `Finish`, incomplete
or stale checkpoints, mismatched filters/headers, corrupt state and untracked targets
are rejected. Apply verifies the root before and after the atomic mutation; preview
checks the current root and journal metadata. Neither mode initializes genesis,
creates missing stores/tables, migrates data, or repairs/unwinds storage. Auxiliary
stores are read-only (RocksDB may create temporary secondary-reader files); applying
also uses the usual MDBX lock file. Logical deletion is not a file-size guarantee.

Restart the follower on the same database afterwards. Non-empty expired storage
must return explicit unavailable-state errors, while subsequent BALs can advance
its preserved commitments. Offline CLI tests are not a live Kurtosis expiry run.

This is a blocking research API: it computes the partial root twice and scans
retained account journals. Async callers must use a blocking task. An in-flight
transition resolved before expiry may need to retry resolution for newly unavailable
storage; no local full-state fallback is permitted. Existing readers retain their
pre-expiry snapshots. Deleted rows free reusable database space after old readers
release their snapshots; they do not guarantee a smaller MDBX file or secure erasure.

## Existing prototype data

There is no implicit migration of completed checkpoints or old journals. A completed
checkpoint without these records cannot establish availability and requires a fresh
partial-state bootstrap. Use a new follower datadir/volume for this version; preserve
old test evidence and data. Do not edit the checkpoint or mark metadata available by
hand. Full-node state tables and their encoding are unchanged.

## Verification

```sh
cargo test -p reth-provider partial_ --lib --offline -j 2
cargo test -p reth-provider partial_state_expiry --lib --offline -j 2 -- --test-threads=1
cargo test -p reth-cli-commands partial_state_expire --lib --offline -j 2 -- --test-threads=1
cargo test -p reth-node-builder partial_availability --lib --offline -j 2
cargo test -p reth-node-builder --test follower --offline -j 2 -- --test-threads=1
python3 -m unittest discover -s scripts/partial-state-follower -p 'test_*.py'
```

Availability tests inject unavailable metadata without deleting data to exercise the
reader boundary. Expiry tests additionally delete real slots and journal before-images,
check atomic failure and unchanged commitments, and replay transitions/reorgs with
expired storage. These are provider tests, not a live Kurtosis expiry test. A normal
follower smoke run does not invoke the expiry API. Automatic retention policy,
bytecode expiry, and verified reacquisition remain separate work. Journal retention
alone is not state expiry.
