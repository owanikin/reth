use crate::{providers::ProviderNodeTypes, DatabaseProvider, ProviderFactory};
use alloy_consensus::{
    constants::{EMPTY_ROOT_HASH, KECCAK_EMPTY},
    BlockHeader,
};
use alloy_eips::{
    eip7928::{compute_block_access_list_hash, AccountChanges},
    BlockNumHash,
};
use alloy_primitives::{keccak256, Address, BlockNumber, Bytes, B256};
use reth_db_api::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    models::{
        BlockNumberAddress, PartialStateAccountBefore, StoredPartialStateAvailability,
        StoredPartialStateCheckpoint, StoredPartialStateTransition,
    },
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_node_types::HeaderTy;
use reth_primitives_traits::{Account, Bytecode, SealedHeader, StorageEntry};
use reth_storage_api::{
    ContractFilter, DBProvider, DatabaseProviderFactory, PartialStateCheckpoint,
    PartialStateCheckpointProvider, PartialStateCheckpointStatus, PartialStateRootProvider,
    PartialStateSnapPivot, PartialStateTransition, PartialStateTransitionProvider,
};
use reth_storage_errors::provider::{
    PartialStateCheckpointError, PartialStateReadError, PartialStateTransitionError, ProviderError,
    ProviderResult,
};
use reth_trie_common::TrieAccount;

const PARTIAL_STATE_CHECKPOINT_KEY: u8 = 0;

impl<N: ProviderNodeTypes> ProviderFactory<N> {
    /// Retains a peer-verified bootstrap header without marking it executed or changing state.
    /// Readers only use it when its hash and root match a complete checkpoint.
    pub fn record_partial_state_header(&self, header: &HeaderTy<N>) -> ProviderResult<()> {
        let provider = self.database_provider_rw()?;
        provider
            .tx_ref()
            .put::<tables::PartialStateHeaders<HeaderTy<N>>>(header.number(), header.clone())?;
        provider.commit()
    }

    /// Commits a verified BAL transition and its RPC header context in the same transaction.
    pub fn apply_partial_state_transition_with_header(
        &self,
        transition: PartialStateTransition<'_>,
        filter: &dyn ContractFilter,
        header: &HeaderTy<N>,
    ) -> ProviderResult<B256> {
        let sealed = SealedHeader::seal_slow(header.clone());
        if sealed.num_hash() != transition.block ||
            sealed.parent_hash() != transition.parent_block_hash ||
            sealed.state_root() != transition.expected_root ||
            sealed.block_access_list_hash() != Some(transition.expected_bal_hash)
        {
            return Err(PartialStateReadError::Unsupported(
                "partial-state transition header mismatch",
            )
            .into())
        }
        let provider = self.database_provider_rw()?;
        let root = apply_partial_state_transition(&provider, transition, filter)?;
        provider
            .tx_ref()
            .put::<tables::PartialStateHeaders<HeaderTy<N>>>(header.number(), header.clone())?;
        provider.commit()?;
        Ok(root)
    }
}

impl<N> PartialStateCheckpointProvider for ProviderFactory<N>
where
    N: ProviderNodeTypes,
{
    fn partial_state_checkpoint(&self) -> ProviderResult<Option<PartialStateCheckpoint>> {
        read_partial_state_checkpoint(&self.database_provider_ro()?)
    }

    fn begin_partial_state_sync(
        &self,
        pivot: PartialStateSnapPivot,
        filter: &dyn ContractFilter,
    ) -> ProviderResult<()> {
        let provider = self.database_provider_rw()?;
        reset_partial_state(&provider)?;
        write_partial_state_checkpoint(
            &provider,
            PartialStateCheckpoint {
                pivot,
                filter_hash: filter.filter_hash(),
                status: PartialStateCheckpointStatus::Syncing,
            },
        )?;
        provider.commit()
    }

    fn complete_partial_state_sync(
        &self,
        pivot: PartialStateSnapPivot,
        filter: &dyn ContractFilter,
    ) -> ProviderResult<PartialStateCheckpoint> {
        let provider = self.database_provider_rw()?;
        let checkpoint = read_partial_state_checkpoint(&provider)?
            .ok_or(PartialStateCheckpointError::Unavailable)?;
        ensure_checkpoint_identity(checkpoint, pivot, filter)?;

        let computed_root = provider.partial_state_root(filter)?;
        if computed_root != pivot.state_root {
            return Err(PartialStateCheckpointError::RootMismatch {
                expected: pivot.state_root,
                computed: computed_root,
            }
            .into())
        }

        if checkpoint.status == PartialStateCheckpointStatus::Syncing {
            initialize_availability(&provider, filter)?;
        }
        let checkpoint = PartialStateCheckpoint {
            pivot,
            filter_hash: filter.filter_hash(),
            status: PartialStateCheckpointStatus::Complete,
        };
        write_partial_state_checkpoint(&provider, checkpoint)?;
        provider.commit()?;
        Ok(checkpoint)
    }
}

impl<N> PartialStateTransitionProvider for ProviderFactory<N>
where
    N: ProviderNodeTypes,
{
    fn apply_partial_state_transition(
        &self,
        transition: PartialStateTransition<'_>,
        filter: &dyn ContractFilter,
    ) -> ProviderResult<B256> {
        let provider = self.database_provider_rw()?;
        let root = apply_partial_state_transition(&provider, transition, filter)?;
        provider.commit()?;
        Ok(root)
    }

    fn revert_partial_state_transition(
        &self,
        block: BlockNumHash,
        filter: &dyn ContractFilter,
    ) -> ProviderResult<PartialStateSnapPivot> {
        let provider = self.database_provider_rw()?;
        let pivot = revert_partial_state_transition(&provider, block, filter)?;
        provider.commit()?;
        Ok(pivot)
    }

    fn prune_partial_state_transition_journal(
        &self,
        block_number: BlockNumber,
    ) -> ProviderResult<usize> {
        let provider = self.database_provider_rw()?;
        let pruned = prune_partial_state_transition_journal(&provider, block_number)?;
        provider.commit()?;
        Ok(pruned)
    }

    fn reset_partial_state(&self) -> ProviderResult<()> {
        let provider = self.database_provider_rw()?;
        reset_partial_state(&provider)?;
        provider.commit()
    }
}

pub(crate) fn read_partial_state_checkpoint<TX, N>(
    provider: &DatabaseProvider<TX, N>,
) -> ProviderResult<Option<PartialStateCheckpoint>>
where
    TX: DbTx + 'static,
    N: ProviderNodeTypes,
{
    Ok(provider.tx_ref().get::<tables::PartialStateCheckpoints>(PARTIAL_STATE_CHECKPOINT_KEY)?.map(
        |stored| PartialStateCheckpoint {
            pivot: PartialStateSnapPivot {
                block_number: stored.block_number,
                block_hash: stored.block_hash,
                state_root: stored.state_root,
            },
            filter_hash: stored.filter_hash,
            status: if stored.sync_complete {
                PartialStateCheckpointStatus::Complete
            } else {
                PartialStateCheckpointStatus::Syncing
            },
        },
    ))
}

fn write_partial_state_checkpoint<TX, N>(
    provider: &DatabaseProvider<TX, N>,
    checkpoint: PartialStateCheckpoint,
) -> ProviderResult<()>
where
    TX: DbTx + DbTxMut + 'static,
    N: ProviderNodeTypes,
{
    provider.tx_ref().put::<tables::PartialStateCheckpoints>(
        PARTIAL_STATE_CHECKPOINT_KEY,
        StoredPartialStateCheckpoint {
            block_number: checkpoint.pivot.block_number,
            block_hash: checkpoint.pivot.block_hash,
            state_root: checkpoint.pivot.state_root,
            filter_hash: checkpoint.filter_hash,
            sync_complete: checkpoint.status == PartialStateCheckpointStatus::Complete,
        },
    )?;
    Ok(())
}

fn ensure_checkpoint_identity(
    checkpoint: PartialStateCheckpoint,
    expected: PartialStateSnapPivot,
    filter: &dyn ContractFilter,
) -> ProviderResult<()> {
    let configured = filter.filter_hash();
    if checkpoint.filter_hash != configured {
        return Err(PartialStateCheckpointError::FilterMismatch {
            expected: checkpoint.filter_hash,
            configured,
        }
        .into())
    }
    if checkpoint.pivot != expected {
        return Err(PartialStateCheckpointError::PivotMismatch {
            expected_number: expected.block_number,
            expected_hash: expected.block_hash,
            expected_root: expected.state_root,
            actual_number: checkpoint.pivot.block_number,
            actual_hash: checkpoint.pivot.block_hash,
            actual_root: checkpoint.pivot.state_root,
        }
        .into())
    }
    Ok(())
}

pub(crate) fn ensure_complete_checkpoint(
    checkpoint: PartialStateCheckpoint,
    expected: PartialStateSnapPivot,
    filter: &dyn ContractFilter,
) -> ProviderResult<()> {
    ensure_checkpoint_identity(checkpoint, expected, filter)?;
    if checkpoint.status != PartialStateCheckpointStatus::Complete {
        return Err(PartialStateCheckpointError::Incomplete {
            block_number: checkpoint.pivot.block_number,
            block_hash: checkpoint.pivot.block_hash,
        }
        .into())
    }
    Ok(())
}

fn apply_partial_state_transition<TX, N>(
    provider: &DatabaseProvider<TX, N>,
    transition: PartialStateTransition<'_>,
    filter: &dyn ContractFilter,
) -> ProviderResult<B256>
where
    TX: DbTx + DbTxMut + Send + Sync + 'static,
    N: ProviderNodeTypes,
{
    if let Some(checkpoint) = read_partial_state_checkpoint(provider)? {
        ensure_complete_checkpoint(
            checkpoint,
            PartialStateSnapPivot {
                block_number: transition.block.number.saturating_sub(1),
                block_hash: transition.parent_block_hash,
                state_root: transition.parent_root,
            },
            filter,
        )?;
    }

    let current_root = provider.partial_state_root(filter)?;
    if current_root != transition.parent_root {
        return Err(PartialStateTransitionError::ParentRootMismatch {
            expected: transition.parent_root,
            computed: current_root,
        }
        .into())
    }

    // Uncheckpointed parent snapshots must establish availability only after root verification.
    if read_partial_state_checkpoint(provider)?.is_none() {
        initialize_availability(provider, filter)?;
    }

    let bal_hash = compute_block_access_list_hash(transition.access_list);
    if bal_hash != transition.expected_bal_hash {
        return Err(PartialStateTransitionError::BalHashMismatch {
            expected: transition.expected_bal_hash,
            computed: bal_hash,
        }
        .into())
    }

    journal_transition(provider, transition)?;

    for changes in transition.access_list {
        apply_account_changes(provider, transition, filter, changes)?;
    }

    let computed_root = provider.partial_state_root(filter)?;
    if computed_root != transition.expected_root {
        return Err(PartialStateTransitionError::ChildRootMismatch {
            expected: transition.expected_root,
            computed: computed_root,
        }
        .into())
    }
    write_partial_state_checkpoint(
        provider,
        PartialStateCheckpoint {
            pivot: PartialStateSnapPivot {
                block_number: transition.block.number,
                block_hash: transition.block.hash,
                state_root: computed_root,
            },
            filter_hash: filter.filter_hash(),
            status: PartialStateCheckpointStatus::Complete,
        },
    )?;
    Ok(computed_root)
}

/// Called only after reconstructing and verifying the snapshot using the configured filter.
fn initialize_availability<TX, N>(
    provider: &DatabaseProvider<TX, N>,
    filter: &dyn ContractFilter,
) -> ProviderResult<()>
where
    TX: DbTx + DbTxMut + Send + Sync + 'static,
    N: ProviderNodeTypes,
{
    let mut cursor = provider.tx_ref().cursor_read::<tables::PartialStateAccounts>()?;
    for entry in cursor.walk(None)? {
        let (hash, account) = entry?;
        let storage_root = provider
            .tx_ref()
            .get::<tables::PartialStateStorageRoots>(hash)?
            .unwrap_or(EMPTY_ROOT_HASH);
        if filter.should_sync_storage_by_hash(&hash) {
            let computed = provider.partial_storage_root_by_hash(hash)?;
            if computed != storage_root {
                return Err(PartialStateReadError::StorageRootMismatch {
                    account_hash: hash,
                    expected: storage_root,
                    computed,
                }
                .into())
            }
        }
        let code_hash = account.get_bytecode_hash();
        let bytecode = code_hash == KECCAK_EMPTY || filter.should_sync_code_by_hash(&hash);
        if bytecode && code_hash != KECCAK_EMPTY {
            let code = provider
                .tx_ref()
                .get::<tables::Bytecodes>(code_hash)?
                .ok_or(PartialStateReadError::MissingBytecode(code_hash))?;
            let computed = keccak256(code.original_bytes());
            if computed != code_hash {
                return Err(PartialStateReadError::BytecodeHashMismatch {
                    expected: code_hash,
                    computed,
                }
                .into())
            }
        }
        provider.tx_ref().put::<tables::PartialStateAvailability>(
            hash,
            StoredPartialStateAvailability {
                storage: storage_root == EMPTY_ROOT_HASH ||
                    filter.should_sync_storage_by_hash(&hash),
                bytecode,
            },
        )?;
    }
    Ok(())
}

fn journal_transition<TX, N>(
    provider: &DatabaseProvider<TX, N>,
    transition: PartialStateTransition<'_>,
) -> ProviderResult<()>
where
    TX: DbTx + DbTxMut + 'static,
    N: ProviderNodeTypes,
{
    if let Some(existing) =
        provider.tx_ref().get::<tables::PartialStateTransitionJournals>(transition.block.number)?
    {
        return Err(PartialStateTransitionError::JournalConflict {
            block_number: transition.block.number,
            existing: existing.block_hash,
            requested: transition.block.hash,
        }
        .into())
    }
    provider.tx_ref().put::<tables::PartialStateTransitionJournals>(
        transition.block.number,
        StoredPartialStateTransition {
            block_hash: transition.block.hash,
            parent_block_hash: transition.parent_block_hash,
            parent_state_root: transition.parent_root,
            state_root: transition.expected_root,
        },
    )?;
    Ok(())
}

fn apply_account_changes<TX, N>(
    provider: &DatabaseProvider<TX, N>,
    transition: PartialStateTransition<'_>,
    filter: &dyn ContractFilter,
    changes: &AccountChanges,
) -> ProviderResult<()>
where
    TX: DbTx + DbTxMut + Send + Sync + 'static,
    N: ProviderNodeTypes,
{
    let has_storage_changes = !changes.storage_changes.is_empty();
    let has_account_changes = !changes.balance_changes.is_empty() ||
        !changes.nonce_changes.is_empty() ||
        !changes.code_changes.is_empty();
    if !has_storage_changes && !has_account_changes {
        return Ok(())
    }

    let address = changes.address;
    let account_hash = keccak256(address);
    journal_account_before(provider, transition.block.number, address, account_hash)?;
    let mut account =
        provider.tx_ref().get::<tables::PartialStateAccounts>(account_hash)?.unwrap_or_default();
    let mut availability = provider
        .tx_ref()
        .get::<tables::PartialStateAvailability>(account_hash)?
        .unwrap_or_default();
    let storage_empty =
        provider.tx_ref().get::<tables::PartialStateStorageRoots>(account_hash)?.is_none();
    let retain_storage =
        filter.should_sync_storage(&address) && (availability.storage || storage_empty);

    if let Some(change) =
        changes.balance_changes.iter().max_by_key(|change| change.block_access_index)
    {
        account.balance = change.post_balance;
    }
    if let Some(change) =
        changes.nonce_changes.iter().max_by_key(|change| change.block_access_index)
    {
        account.nonce = change.new_nonce;
    }
    if let Some(change) = changes.code_changes.iter().max_by_key(|change| change.block_access_index)
    {
        let code_hash = keccak256(&change.new_code);
        account.bytecode_hash = (code_hash != KECCAK_EMPTY).then_some(code_hash);
        if filter.should_sync_code(&address) && !change.new_code.is_empty() {
            let bytecode = Bytecode::new_raw_checked(Bytes::copy_from_slice(&change.new_code))
                .map_err(ProviderError::other)?;
            provider.tx_ref().put::<tables::Bytecodes>(code_hash, bytecode)?;
        }
        availability.bytecode = filter.should_sync_code(&address) || code_hash == KECCAK_EMPTY;
    }

    let storage_root = if has_storage_changes {
        if retain_storage {
            apply_tracked_storage_changes(
                provider,
                transition.block.number,
                address,
                account_hash,
                changes,
            )?;
            provider.partial_storage_root_by_hash(account_hash)?
        } else {
            let resolved = transition.resolved_accounts.get(&address).ok_or_else(|| {
                ProviderError::from(PartialStateTransitionError::AccountCommitmentUnavailable {
                    address,
                    state_root: transition.expected_root,
                })
            })?;

            let Some(resolved) = resolved else {
                if !account.is_empty() {
                    return Err(PartialStateTransitionError::ResolvedAccountMismatch {
                        address,
                        state_root: transition.expected_root,
                    }
                    .into())
                }
                delete_account(provider, account_hash)?;
                return Ok(())
            };
            if !resolved_account_matches_bal(&account, resolved) {
                return Err(PartialStateTransitionError::ResolvedAccountMismatch {
                    address,
                    state_root: transition.expected_root,
                }
                .into())
            }
            account = Account::from(*resolved);
            resolved.storage_root
        }
    } else if retain_storage {
        provider.partial_storage_root_by_hash(account_hash)?
    } else {
        provider
            .tx_ref()
            .get::<tables::PartialStateStorageRoots>(account_hash)?
            .unwrap_or(EMPTY_ROOT_HASH)
    };

    availability.storage = retain_storage || storage_root == EMPTY_ROOT_HASH;
    availability.bytecode |= account.bytecode_hash.is_none();
    if account.is_empty() && storage_root == EMPTY_ROOT_HASH {
        delete_account(provider, account_hash)?;
    } else {
        provider.tx_ref().put::<tables::PartialStateAccounts>(account_hash, account)?;
        write_storage_commitment(provider, account_hash, storage_root)?;
        provider.tx_ref().put::<tables::PartialStateAvailability>(account_hash, availability)?;
    }
    Ok(())
}

fn journal_account_before<TX, N>(
    provider: &DatabaseProvider<TX, N>,
    block_number: BlockNumber,
    address: Address,
    account_hash: B256,
) -> ProviderResult<()>
where
    TX: DbTx + DbTxMut + 'static,
    N: ProviderNodeTypes,
{
    let mut journal =
        provider.tx_ref().cursor_dup_write::<tables::PartialStateAccountChangeSets>()?;
    if journal.seek_by_key_subkey(block_number, address)?.is_some() {
        return Ok(())
    }

    let account = provider.tx_ref().get::<tables::PartialStateAccounts>(account_hash)?;
    let availability = if account.is_some() {
        provider
            .tx_ref()
            .get::<tables::PartialStateAvailability>(account_hash)?
            .ok_or(PartialStateReadError::MissingAvailability(account_hash))?
    } else {
        StoredPartialStateAvailability::default()
    };
    provider.tx_ref().put::<tables::PartialStateAvailabilityChangeSets>(
        BlockNumberAddress((block_number, address)),
        availability,
    )?;
    journal.upsert(
        block_number,
        &PartialStateAccountBefore {
            address,
            account,
            storage_root: provider
                .tx_ref()
                .get::<tables::PartialStateStorageRoots>(account_hash)?,
        },
    )?;
    Ok(())
}

fn apply_tracked_storage_changes<TX, N>(
    provider: &DatabaseProvider<TX, N>,
    block_number: BlockNumber,
    address: Address,
    account_hash: B256,
    changes: &AccountChanges,
) -> ProviderResult<()>
where
    TX: DbTx + DbTxMut + 'static,
    N: ProviderNodeTypes,
{
    let mut cursor = provider.tx_ref().cursor_dup_write::<tables::PartialStateStorages>()?;
    let mut journal =
        provider.tx_ref().cursor_dup_write::<tables::PartialStateStorageChangeSets>()?;
    let journal_key = BlockNumberAddress((block_number, address));
    for slot_changes in &changes.storage_changes {
        let Some(change) =
            slot_changes.changes.iter().max_by_key(|change| change.block_access_index)
        else {
            continue
        };
        let slot_hash = keccak256(slot_changes.slot.to_be_bytes::<32>());
        let previous = cursor
            .seek_by_key_subkey(account_hash, slot_hash)?
            .filter(|entry| entry.key == slot_hash);
        if journal
            .seek_by_key_subkey(journal_key, slot_hash)?
            .is_none_or(|entry| entry.key != slot_hash)
        {
            journal.upsert(
                journal_key,
                &StorageEntry::new(
                    slot_hash,
                    previous.map_or_else(Default::default, |entry| entry.value),
                ),
            )?;
        }
        if previous.is_some() {
            cursor.delete_current()?;
        }
        if !change.new_value.is_zero() {
            cursor.upsert(account_hash, &StorageEntry::new(slot_hash, change.new_value))?;
        }
    }
    Ok(())
}

fn revert_partial_state_transition<TX, N>(
    provider: &DatabaseProvider<TX, N>,
    block: BlockNumHash,
    filter: &dyn ContractFilter,
) -> ProviderResult<PartialStateSnapPivot>
where
    TX: DbTx + DbTxMut + Send + Sync + 'static,
    N: ProviderNodeTypes,
{
    let Some(parent_number) = block.number.checked_sub(1) else {
        return Err(PartialStateTransitionError::CannotRevertGenesis.into())
    };
    let journal = provider
        .tx_ref()
        .get::<tables::PartialStateTransitionJournals>(block.number)?
        .ok_or(PartialStateTransitionError::JournalNotFound { block_number: block.number })?;
    if journal.block_hash != block.hash {
        return Err(PartialStateTransitionError::JournalBlockHashMismatch {
            block_number: block.number,
            expected: block.hash,
            actual: journal.block_hash,
        }
        .into())
    }

    if let Some(checkpoint) = read_partial_state_checkpoint(provider)? {
        ensure_complete_checkpoint(
            checkpoint,
            PartialStateSnapPivot {
                block_number: block.number,
                block_hash: block.hash,
                state_root: journal.state_root,
            },
            filter,
        )?;
    }

    let current_root = provider.partial_state_root(filter)?;
    if current_root != journal.state_root {
        return Err(PartialStateTransitionError::RollbackRootMismatch {
            expected: journal.state_root,
            computed: current_root,
        }
        .into())
    }

    let account_changes = provider
        .tx_ref()
        .cursor_read::<tables::PartialStateAccountChangeSets>()?
        .walk_range(block.number..=block.number)?
        .map(|entry| entry.map(|(_, change)| change))
        .collect::<Result<Vec<_>, _>>()?;
    let storage_changes = provider
        .tx_ref()
        .cursor_dup_read::<tables::PartialStateStorageChangeSets>()?
        .walk_range(BlockNumberAddress::range(block.number..=block.number))?
        .collect::<Result<Vec<_>, _>>()?;

    restore_partial_storage(provider, &storage_changes)?;
    for change in &account_changes {
        let account_hash = keccak256(change.address);
        if let Some(account) = change.account {
            provider.tx_ref().put::<tables::PartialStateAccounts>(account_hash, account)?;
            let availability = provider
                .tx_ref()
                .get::<tables::PartialStateAvailabilityChangeSets>(BlockNumberAddress((
                    block.number,
                    change.address,
                )))?
                .ok_or(PartialStateReadError::MissingAvailability(account_hash))?;
            provider
                .tx_ref()
                .put::<tables::PartialStateAvailability>(account_hash, availability)?;
        } else {
            provider.tx_ref().delete::<tables::PartialStateAccounts>(account_hash, None)?;
            provider.tx_ref().delete::<tables::PartialStateAvailability>(account_hash, None)?;
        }
        if let Some(storage_root) = change.storage_root {
            provider
                .tx_ref()
                .put::<tables::PartialStateStorageRoots>(account_hash, storage_root)?;
        } else {
            provider.tx_ref().delete::<tables::PartialStateStorageRoots>(account_hash, None)?;
        }
    }

    let computed_root = provider.partial_state_root(filter)?;
    if computed_root != journal.parent_state_root {
        return Err(PartialStateTransitionError::RollbackRootMismatch {
            expected: journal.parent_state_root,
            computed: computed_root,
        }
        .into())
    }
    delete_transition_journal(provider, block.number, &account_changes)?;

    let pivot = PartialStateSnapPivot {
        block_number: parent_number,
        block_hash: journal.parent_block_hash,
        state_root: journal.parent_state_root,
    };
    write_partial_state_checkpoint(
        provider,
        PartialStateCheckpoint {
            pivot,
            filter_hash: filter.filter_hash(),
            status: PartialStateCheckpointStatus::Complete,
        },
    )?;

    Ok(pivot)
}

fn restore_partial_storage<TX, N>(
    provider: &DatabaseProvider<TX, N>,
    changes: &[(BlockNumberAddress, StorageEntry)],
) -> ProviderResult<()>
where
    TX: DbTx + DbTxMut + 'static,
    N: ProviderNodeTypes,
{
    let mut storage = provider.tx_ref().cursor_dup_write::<tables::PartialStateStorages>()?;
    for (block_address, entry) in changes {
        let account_hash = keccak256(block_address.address());
        if storage
            .seek_by_key_subkey(account_hash, entry.key)?
            .is_some_and(|current| current.key == entry.key)
        {
            storage.delete_current()?;
        }
        if !entry.value.is_zero() {
            storage.upsert(account_hash, entry)?;
        }
    }
    Ok(())
}

fn prune_partial_state_transition_journal<TX, N>(
    provider: &DatabaseProvider<TX, N>,
    block_number: BlockNumber,
) -> ProviderResult<usize>
where
    TX: DbTx + DbTxMut + 'static,
    N: ProviderNodeTypes,
{
    let blocks = provider
        .tx_ref()
        .cursor_read::<tables::PartialStateTransitionJournals>()?
        .walk_range(..block_number)?
        .map(|entry| entry.map(|(number, _)| number))
        .collect::<Result<Vec<_>, _>>()?;

    for number in &blocks {
        let account_changes = provider
            .tx_ref()
            .cursor_read::<tables::PartialStateAccountChangeSets>()?
            .walk_range(*number..=*number)?
            .map(|entry| entry.map(|(_, change)| change))
            .collect::<Result<Vec<_>, _>>()?;
        delete_transition_journal(provider, *number, &account_changes)?;
    }
    // Keep the oldest revertible checkpoint's EVM history as well as the journal window.
    let mut headers =
        provider.tx_ref().cursor_write::<tables::PartialStateHeaders<HeaderTy<N>>>()?;
    while let Some((number, _)) = headers.first()? {
        if number >= block_number.saturating_sub(257) {
            break
        }
        headers.delete_current()?;
    }
    Ok(blocks.len())
}

fn delete_transition_journal<TX, N>(
    provider: &DatabaseProvider<TX, N>,
    block_number: BlockNumber,
    account_changes: &[PartialStateAccountBefore],
) -> ProviderResult<()>
where
    TX: DbTx + DbTxMut + 'static,
    N: ProviderNodeTypes,
{
    for change in account_changes {
        provider.tx_ref().delete::<tables::PartialStateAvailabilityChangeSets>(
            BlockNumberAddress((block_number, change.address)),
            None,
        )?;
        provider.tx_ref().delete::<tables::PartialStateStorageChangeSets>(
            BlockNumberAddress((block_number, change.address)),
            None,
        )?;
    }
    provider.tx_ref().delete::<tables::PartialStateAccountChangeSets>(block_number, None)?;
    provider.tx_ref().delete::<tables::PartialStateTransitionJournals>(block_number, None)?;
    Ok(())
}

fn reset_partial_state<TX, N>(provider: &DatabaseProvider<TX, N>) -> ProviderResult<()>
where
    TX: DbTx + DbTxMut + 'static,
    N: ProviderNodeTypes,
{
    // Header context survives resync selection; readers hash-check it against the new checkpoint.
    // Bytecodes are content-addressed and shared with canonical state, so old entries are harmless.
    provider.tx_ref().clear::<tables::PartialStateCheckpoints>()?;
    provider.tx_ref().clear::<tables::PartialStateStorageChangeSets>()?;
    provider.tx_ref().clear::<tables::PartialStateAccountChangeSets>()?;
    provider.tx_ref().clear::<tables::PartialStateAvailabilityChangeSets>()?;
    provider.tx_ref().clear::<tables::PartialStateTransitionJournals>()?;
    provider.tx_ref().clear::<tables::PartialStateStorages>()?;
    provider.tx_ref().clear::<tables::PartialStateStorageRoots>()?;
    provider.tx_ref().clear::<tables::PartialStateAccounts>()?;
    provider.tx_ref().clear::<tables::PartialStateAvailability>()?;
    Ok(())
}

fn delete_account<TX, N>(
    provider: &DatabaseProvider<TX, N>,
    account_hash: B256,
) -> ProviderResult<()>
where
    TX: DbTx + DbTxMut + 'static,
    N: ProviderNodeTypes,
{
    provider.tx_ref().delete::<tables::PartialStateAccounts>(account_hash, None)?;
    provider.tx_ref().delete::<tables::PartialStateStorageRoots>(account_hash, None)?;
    provider.tx_ref().delete::<tables::PartialStateAvailability>(account_hash, None)?;
    let mut storage = provider.tx_ref().cursor_dup_write::<tables::PartialStateStorages>()?;
    if storage.seek_exact(account_hash)?.is_some() {
        storage.delete_current_duplicates()?;
    }
    Ok(())
}

fn write_storage_commitment<TX, N>(
    provider: &DatabaseProvider<TX, N>,
    account_hash: B256,
    storage_root: B256,
) -> ProviderResult<()>
where
    TX: DbTx + DbTxMut + 'static,
    N: ProviderNodeTypes,
{
    if storage_root == EMPTY_ROOT_HASH {
        provider.tx_ref().delete::<tables::PartialStateStorageRoots>(account_hash, None)?;
    } else {
        provider.tx_ref().put::<tables::PartialStateStorageRoots>(account_hash, storage_root)?;
    }
    Ok(())
}

fn resolved_account_matches_bal(account: &Account, resolved: &TrieAccount) -> bool {
    account.nonce == resolved.nonce &&
        account.balance == resolved.balance &&
        account.get_bytecode_hash() == resolved.code_hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_test_provider_factory;
    use alloy_consensus::Header;
    use alloy_eip7928::{BalanceChange, CodeChange, NonceChange, SlotChanges, StorageChange};
    use alloy_eips::eip7928::{compute_block_access_list_hash, BlockAccessList};
    use alloy_primitives::{address, U256};
    use reth_db_api::{cursor::DbDupCursorRO, transaction::DbTx};
    use reth_storage_api::{
        ConfiguredContractFilter, DBProvider, PartialStateCheckpointProvider,
        PartialStateCheckpointStatus, PartialStateResolvedAccounts, PartialStateSnapWriter,
    };
    use reth_trie::root::{state_root_unsorted, storage_root};

    #[test]
    fn partial_state_call_context_commits_and_reverts_with_checkpoint() {
        let factory = create_test_provider_factory();
        let filter = ConfiguredContractFilter::new([]);
        let parent =
            SealedHeader::seal_slow(Header { state_root: EMPTY_ROOT_HASH, ..Default::default() });
        let pivot = PartialStateSnapPivot {
            block_number: 0,
            block_hash: parent.hash(),
            state_root: parent.state_root,
        };
        factory.record_partial_state_header(&parent).unwrap();
        factory.begin_partial_state_sync(pivot, &filter).unwrap();
        factory.complete_partial_state_sync(pivot, &filter).unwrap();

        let access_list = BlockAccessList::default();
        let resolved_accounts = PartialStateResolvedAccounts::default();
        let child = SealedHeader::seal_slow(Header {
            number: 1,
            parent_hash: parent.hash(),
            state_root: EMPTY_ROOT_HASH,
            block_access_list_hash: Some(compute_block_access_list_hash(&access_list)),
            ..Default::default()
        });
        let transition = PartialStateTransition {
            block: child.num_hash(),
            parent_block_hash: parent.hash(),
            parent_root: parent.state_root,
            expected_root: child.state_root,
            expected_bal_hash: child.block_access_list_hash.unwrap(),
            access_list: &access_list,
            resolved_accounts: &resolved_accounts,
        };

        assert!(factory
            .apply_partial_state_transition_with_header(transition, &filter, &parent)
            .is_err());
        assert_eq!(factory.partial_state_checkpoint().unwrap().unwrap().pivot, pivot);
        assert!(factory
            .database_provider_ro()
            .unwrap()
            .tx_ref()
            .get::<tables::PartialStateHeaders>(1)
            .unwrap()
            .is_none());

        factory.apply_partial_state_transition_with_header(transition, &filter, &child).unwrap();
        let (header, snapshot) = factory
            .partial_state_follower_reader(alloy_eips::BlockId::latest(), &filter)
            .unwrap()
            .with_call_context()
            .unwrap();
        assert_eq!(header, child);
        assert_eq!(snapshot.checkpoint().pivot.block_hash, child.hash());

        factory.revert_partial_state_transition(child.num_hash(), &filter).unwrap();
        let (header, _) = factory
            .partial_state_follower_reader(alloy_eips::BlockId::latest(), &filter)
            .unwrap()
            .with_call_context()
            .unwrap();
        assert_eq!(header, parent);
        assert_eq!(snapshot.checkpoint().pivot.block_hash, child.hash());

        // A correctly identified header is not enough: root failure must roll back its context too.
        let bad_child = SealedHeader::seal_slow(Header {
            state_root: B256::repeat_byte(0x99),
            ..child.clone_header()
        });
        let error = factory
            .apply_partial_state_transition_with_header(
                PartialStateTransition {
                    block: bad_child.num_hash(),
                    expected_root: bad_child.state_root,
                    ..transition
                },
                &filter,
                &bad_child,
            )
            .unwrap_err();
        assert!(matches!(error, ProviderError::PartialStateTransition(error)
            if matches!(*error, PartialStateTransitionError::ChildRootMismatch { .. })));
        assert_eq!(factory.partial_state_checkpoint().unwrap().unwrap().pivot, pivot);
        assert_eq!(
            factory
                .database_provider_ro()
                .unwrap()
                .tx_ref()
                .get::<tables::PartialStateHeaders>(1)
                .unwrap(),
            Some(child.clone_header())
        );
    }

    #[test]
    fn partial_state_call_context_prunes_beyond_rollback_blockhash_window() {
        let factory = create_test_provider_factory();
        let provider = factory.database_provider_rw().unwrap();
        for number in 0..=300 {
            provider
                .tx_ref()
                .put::<tables::PartialStateHeaders>(number, Header { number, ..Default::default() })
                .unwrap();
        }
        provider.commit().unwrap();

        // Journal 280 allows rollback to checkpoint 279, whose oldest BLOCKHASH is 23.
        assert_eq!(factory.prune_partial_state_transition_journal(280).unwrap(), 0);
        let provider = factory.database_provider_ro().unwrap();
        assert_eq!(provider.tx_ref().entries::<tables::PartialStateHeaders>().unwrap(), 278);
        assert!(provider.tx_ref().get::<tables::PartialStateHeaders>(22).unwrap().is_none());
        assert!(provider.tx_ref().get::<tables::PartialStateHeaders>(23).unwrap().is_some());
        assert!(provider.tx_ref().get::<tables::PartialStateHeaders>(300).unwrap().is_some());
    }

    #[test]
    fn persists_partial_state_checkpoint_lifecycle() {
        let factory = create_test_provider_factory();
        let address = address!("0000000000000000000000000000000000000001");
        let account_hash = keccak256(address);
        let stale_hash = B256::repeat_byte(0xaa);
        let account = TrieAccount {
            balance: U256::from(1),
            storage_root: EMPTY_ROOT_HASH,
            code_hash: KECCAK_EMPTY,
            ..Default::default()
        };
        let pivot = PartialStateSnapPivot {
            block_number: 7,
            block_hash: B256::repeat_byte(0x77),
            state_root: state_root_unsorted([(account_hash, account)]),
        };
        let filter = ConfiguredContractFilter::new([address]);

        let provider = factory.database_provider_rw().unwrap();
        provider
            .partial_state_snap_writer()
            .write_account(stale_hash, TrieAccount::default())
            .unwrap();
        provider.commit().unwrap();

        factory.begin_partial_state_sync(pivot, &filter).unwrap();
        assert_eq!(
            factory.partial_state_checkpoint().unwrap(),
            Some(PartialStateCheckpoint {
                pivot,
                filter_hash: filter.filter_hash(),
                status: PartialStateCheckpointStatus::Syncing,
            })
        );
        assert_eq!(
            factory
                .database_provider_ro()
                .unwrap()
                .tx_ref()
                .get::<tables::PartialStateAccounts>(stale_hash)
                .unwrap(),
            None
        );

        let provider = factory.database_provider_rw().unwrap();
        provider.partial_state_snap_writer().write_account(account_hash, account).unwrap();
        provider.commit().unwrap();

        let checkpoint = factory.complete_partial_state_sync(pivot, &filter).unwrap();
        assert_eq!(
            checkpoint,
            PartialStateCheckpoint {
                pivot,
                filter_hash: filter.filter_hash(),
                status: PartialStateCheckpointStatus::Complete,
            }
        );
        assert_eq!(factory.partial_state_checkpoint().unwrap(), Some(checkpoint));

        factory.reset_partial_state().unwrap();
        assert_eq!(factory.partial_state_checkpoint().unwrap(), None);
    }

    #[test]
    fn partial_state_checkpoint_rejects_filter_changes() {
        let factory = create_test_provider_factory();
        let pivot = PartialStateSnapPivot {
            block_number: 0,
            block_hash: B256::repeat_byte(0x10),
            state_root: EMPTY_ROOT_HASH,
        };
        let original =
            ConfiguredContractFilter::new([address!("0000000000000000000000000000000000000001")]);
        let changed =
            ConfiguredContractFilter::new([address!("0000000000000000000000000000000000000002")]);

        factory.begin_partial_state_sync(pivot, &original).unwrap();
        let err = factory.complete_partial_state_sync(pivot, &changed).unwrap_err();
        assert!(matches!(
            err,
            ProviderError::PartialStateCheckpoint(error)
                if matches!(*error, PartialStateCheckpointError::FilterMismatch { .. })
        ));
        assert_eq!(
            factory.partial_state_checkpoint().unwrap(),
            Some(PartialStateCheckpoint {
                pivot,
                filter_hash: original.filter_hash(),
                status: PartialStateCheckpointStatus::Syncing,
            })
        );
    }

    #[test]
    fn applies_mixed_bal_and_matches_complete_reference_root() {
        let factory = create_test_provider_factory();
        let tracked = address!("0000000000000000000000000000000000000001");
        let untracked = address!("0000000000000000000000000000000000000002");
        let untouched = address!("0000000000000000000000000000000000000003");
        let tracked_hash = keccak256(tracked);
        let untracked_hash = keccak256(untracked);
        let untouched_hash = keccak256(untouched);
        let tracked_slot = U256::from(1);
        let untracked_slot = U256::from(2);
        let tracked_slot_hash = keccak256(tracked_slot.to_be_bytes::<32>());
        let untracked_slot_hash = keccak256(untracked_slot.to_be_bytes::<32>());
        let tracked_parent_value = U256::from(10);
        let tracked_child_value = U256::from(11);
        let untracked_parent_value = U256::from(20);
        let untracked_child_value = U256::from(21);
        let tracked_parent_storage_root = storage_root([(tracked_slot_hash, tracked_parent_value)]);
        let tracked_child_storage_root = storage_root([(tracked_slot_hash, tracked_child_value)]);
        let untracked_parent_storage_root =
            storage_root([(untracked_slot_hash, untracked_parent_value)]);
        let untracked_child_storage_root =
            storage_root([(untracked_slot_hash, untracked_child_value)]);
        let tracked_code = Bytes::from_static(&[0x60, 0x00]);
        let untracked_code = Bytes::from_static(&[0x60, 0x01]);
        let tracked_code_hash = keccak256(&tracked_code);
        let untracked_code_hash = keccak256(&untracked_code);

        let tracked_parent = TrieAccount {
            nonce: 1,
            balance: U256::from(100),
            storage_root: tracked_parent_storage_root,
            code_hash: KECCAK_EMPTY,
        };
        let untracked_parent = TrieAccount {
            nonce: 2,
            balance: U256::from(200),
            storage_root: untracked_parent_storage_root,
            code_hash: KECCAK_EMPTY,
        };
        let untouched_account = TrieAccount {
            nonce: 3,
            balance: U256::from(300),
            storage_root: EMPTY_ROOT_HASH,
            code_hash: KECCAK_EMPTY,
        };
        let parent_root = state_root_unsorted([
            (tracked_hash, tracked_parent),
            (untracked_hash, untracked_parent),
            (untouched_hash, untouched_account),
        ]);

        let provider = factory.database_provider_rw().unwrap();
        {
            let mut writer = provider.partial_state_snap_writer();
            writer.write_account(tracked_hash, tracked_parent).unwrap();
            writer.write_storage(tracked_hash, tracked_slot_hash, tracked_parent_value).unwrap();
            writer.write_account(untracked_hash, untracked_parent).unwrap();
            writer.write_account(untouched_hash, untouched_account).unwrap();
        }
        provider.commit().unwrap();

        let tracked_child = TrieAccount {
            nonce: 1,
            balance: U256::from(101),
            storage_root: tracked_child_storage_root,
            code_hash: tracked_code_hash,
        };
        let untracked_child = TrieAccount {
            nonce: 3,
            balance: U256::from(201),
            storage_root: untracked_child_storage_root,
            code_hash: untracked_code_hash,
        };
        let expected_root = state_root_unsorted([
            (tracked_hash, tracked_child),
            (untracked_hash, untracked_child),
            (untouched_hash, untouched_account),
        ]);
        let access_list: BlockAccessList = vec![
            AccountChanges::new(tracked)
                .with_storage_change(SlotChanges::new(
                    tracked_slot,
                    vec![StorageChange::new(1, tracked_child_value)],
                ))
                .with_balance_change(BalanceChange::new(1, tracked_child.balance))
                .with_code_change(CodeChange::new(1, tracked_code)),
            AccountChanges::new(untracked)
                .with_storage_change(SlotChanges::new(
                    untracked_slot,
                    vec![StorageChange::new(1, untracked_child_value)],
                ))
                .with_balance_change(BalanceChange::new(1, untracked_child.balance))
                .with_nonce_change(NonceChange::new(1, untracked_child.nonce))
                .with_code_change(CodeChange::new(1, untracked_code)),
            AccountChanges::new(untouched).with_storage_read(U256::from(3)),
        ];
        let resolved_accounts =
            PartialStateResolvedAccounts::from([(untracked, Some(untracked_child))]);
        let filter = ConfiguredContractFilter::new([tracked]);

        let root = factory
            .apply_partial_state_transition(
                PartialStateTransition {
                    block: BlockNumHash::new(1, B256::repeat_byte(0x11)),
                    parent_block_hash: B256::repeat_byte(0x10),
                    parent_root,
                    expected_root,
                    expected_bal_hash: compute_block_access_list_hash(&access_list),
                    access_list: &access_list,
                    resolved_accounts: &resolved_accounts,
                },
                &filter,
            )
            .unwrap();
        assert_eq!(root, expected_root);
        assert_eq!(factory.partial_state_root(&filter).unwrap(), expected_root);
        assert_eq!(
            factory.partial_state_checkpoint().unwrap(),
            Some(PartialStateCheckpoint {
                pivot: PartialStateSnapPivot {
                    block_number: 1,
                    block_hash: B256::repeat_byte(0x11),
                    state_root: expected_root,
                },
                filter_hash: filter.filter_hash(),
                status: PartialStateCheckpointStatus::Complete,
            })
        );

        let provider = factory.database_provider_ro().unwrap();
        assert_eq!(
            provider.tx_ref().get::<tables::PartialStateAccounts>(tracked_hash).unwrap(),
            Some(Account::from(tracked_child))
        );
        assert_eq!(
            provider.tx_ref().get::<tables::PartialStateAccounts>(untracked_hash).unwrap(),
            Some(Account::from(untracked_child))
        );
        assert_eq!(
            provider.tx_ref().get::<tables::PartialStateStorageRoots>(tracked_hash).unwrap(),
            Some(tracked_child_storage_root)
        );
        assert_eq!(
            provider.tx_ref().get::<tables::PartialStateStorageRoots>(untracked_hash).unwrap(),
            Some(untracked_child_storage_root)
        );

        let mut storage =
            provider.tx_ref().cursor_dup_read::<tables::PartialStateStorages>().unwrap();
        assert_eq!(
            storage.seek_by_key_subkey(tracked_hash, tracked_slot_hash).unwrap(),
            Some(StorageEntry::new(tracked_slot_hash, tracked_child_value))
        );
        assert_eq!(storage.seek_by_key_subkey(untracked_hash, B256::ZERO).unwrap(), None);
        assert!(provider.tx_ref().get::<tables::Bytecodes>(tracked_code_hash).unwrap().is_some());
        assert_eq!(provider.tx_ref().get::<tables::Bytecodes>(untracked_code_hash).unwrap(), None);
    }

    #[test]
    fn preserves_partial_commitments_across_canonical_state_advancement() {
        let factory = create_test_provider_factory();
        let address = address!("0000000000000000000000000000000000000001");
        let account_hash = keccak256(address);
        let slot = U256::from(1);
        let slot_hash = keccak256(slot.to_be_bytes::<32>());
        let parent_value = U256::from(10);
        let first_value = U256::from(11);
        let second_value = U256::from(12);
        let parent_account = TrieAccount {
            balance: U256::from(100),
            storage_root: storage_root([(slot_hash, parent_value)]),
            code_hash: KECCAK_EMPTY,
            ..Default::default()
        };
        let first_account = TrieAccount {
            balance: U256::from(101),
            storage_root: storage_root([(slot_hash, first_value)]),
            code_hash: KECCAK_EMPTY,
            ..Default::default()
        };
        let second_account = TrieAccount {
            balance: U256::from(102),
            storage_root: storage_root([(slot_hash, second_value)]),
            code_hash: KECCAK_EMPTY,
            ..Default::default()
        };
        let parent_root = state_root_unsorted([(account_hash, parent_account)]);
        let first_root = state_root_unsorted([(account_hash, first_account)]);
        let second_root = state_root_unsorted([(account_hash, second_account)]);

        let provider = factory.database_provider_rw().unwrap();
        {
            let mut writer = provider.partial_state_snap_writer();
            writer.write_account(account_hash, parent_account).unwrap();
            writer.write_storage(account_hash, slot_hash, parent_value).unwrap();
        }
        provider.commit().unwrap();

        let filter = ConfiguredContractFilter::new([address]);
        let resolved_accounts = PartialStateResolvedAccounts::default();
        let first_access_list = vec![AccountChanges::new(address)
            .with_storage_change(SlotChanges::new(slot, vec![StorageChange::new(1, first_value)]))
            .with_balance_change(BalanceChange::new(1, first_account.balance))];
        factory
            .apply_partial_state_transition(
                PartialStateTransition {
                    block: BlockNumHash::new(1, B256::repeat_byte(0x11)),
                    parent_block_hash: B256::repeat_byte(0x10),
                    parent_root,
                    expected_root: first_root,
                    expected_bal_hash: compute_block_access_list_hash(&first_access_list),
                    access_list: &first_access_list,
                    resolved_accounts: &resolved_accounts,
                },
                &filter,
            )
            .unwrap();

        // Canonical execution advances the regular hashed-state tables independently.
        let provider = factory.database_provider_rw().unwrap();
        provider
            .tx_ref()
            .put::<tables::HashedAccounts>(
                account_hash,
                Account { balance: U256::from(999), ..Default::default() },
            )
            .unwrap();
        provider
            .tx_ref()
            .put::<tables::HashedStorages>(
                account_hash,
                StorageEntry::new(slot_hash, U256::from(999)),
            )
            .unwrap();
        provider.commit().unwrap();

        assert_eq!(factory.partial_state_root(&filter).unwrap(), first_root);

        let second_access_list = vec![AccountChanges::new(address)
            .with_storage_change(SlotChanges::new(slot, vec![StorageChange::new(1, second_value)]))
            .with_balance_change(BalanceChange::new(1, second_account.balance))];
        factory
            .apply_partial_state_transition(
                PartialStateTransition {
                    block: BlockNumHash::new(2, B256::repeat_byte(0x12)),
                    parent_block_hash: B256::repeat_byte(0x11),
                    parent_root: first_root,
                    expected_root: second_root,
                    expected_bal_hash: compute_block_access_list_hash(&second_access_list),
                    access_list: &second_access_list,
                    resolved_accounts: &resolved_accounts,
                },
                &filter,
            )
            .unwrap();

        assert_eq!(factory.partial_state_root(&filter).unwrap(), second_root);
        assert_eq!(factory.prune_partial_state_transition_journal(2).unwrap(), 1);

        let provider = factory.database_provider_ro().unwrap();
        assert!(provider
            .tx_ref()
            .get::<tables::PartialStateAvailabilityChangeSets>(BlockNumberAddress((1, address)))
            .unwrap()
            .is_none());
        assert!(provider
            .tx_ref()
            .get::<tables::PartialStateAvailabilityChangeSets>(BlockNumberAddress((2, address)))
            .unwrap()
            .is_some());
        assert_eq!(
            provider.tx_ref().get::<tables::PartialStateTransitionJournals>(1).unwrap(),
            None
        );
        assert!(provider
            .tx_ref()
            .get::<tables::PartialStateTransitionJournals>(2)
            .unwrap()
            .is_some());
        assert_eq!(
            provider
                .tx_ref()
                .cursor_dup_read::<tables::PartialStateAccountChangeSets>()
                .unwrap()
                .seek_by_key_subkey(1, address)
                .unwrap(),
            None
        );
        assert_eq!(
            provider
                .tx_ref()
                .cursor_dup_read::<tables::PartialStateStorageChangeSets>()
                .unwrap()
                .seek_by_key_subkey(BlockNumberAddress((1, address)), slot_hash)
                .unwrap(),
            None
        );
    }

    #[test]
    fn reverts_journaled_partial_state_transition() {
        let factory = create_test_provider_factory();
        let address = address!("0000000000000000000000000000000000000001");
        let account_hash = keccak256(address);
        let slot = U256::from(1);
        let slot_hash = keccak256(slot.to_be_bytes::<32>());
        let parent_value = U256::from(10);
        let child_value = U256::from(11);
        let parent_account = TrieAccount {
            balance: U256::from(100),
            storage_root: storage_root([(slot_hash, parent_value)]),
            code_hash: KECCAK_EMPTY,
            ..Default::default()
        };
        let child_account = TrieAccount {
            balance: U256::from(101),
            storage_root: storage_root([(slot_hash, child_value)]),
            code_hash: KECCAK_EMPTY,
            ..Default::default()
        };
        let parent_root = state_root_unsorted([(account_hash, parent_account)]);
        let child_root = state_root_unsorted([(account_hash, child_account)]);
        let parent_block_hash = B256::repeat_byte(0x10);
        let child_block = BlockNumHash::new(1, B256::repeat_byte(0x11));

        let provider = factory.database_provider_rw().unwrap();
        {
            let mut writer = provider.partial_state_snap_writer();
            writer.write_account(account_hash, parent_account).unwrap();
            writer.write_storage(account_hash, slot_hash, parent_value).unwrap();
        }
        provider.commit().unwrap();

        let access_list = vec![AccountChanges::new(address)
            .with_storage_change(SlotChanges::new(slot, vec![StorageChange::new(1, child_value)]))
            .with_balance_change(BalanceChange::new(1, child_account.balance))];
        let filter = ConfiguredContractFilter::new([address]);
        factory
            .apply_partial_state_transition(
                PartialStateTransition {
                    block: child_block,
                    parent_block_hash,
                    parent_root,
                    expected_root: child_root,
                    expected_bal_hash: compute_block_access_list_hash(&access_list),
                    access_list: &access_list,
                    resolved_accounts: &PartialStateResolvedAccounts::default(),
                },
                &filter,
            )
            .unwrap();

        let provider = factory.database_provider_ro().unwrap();
        assert_eq!(
            provider.tx_ref().get::<tables::PartialStateTransitionJournals>(1).unwrap(),
            Some(StoredPartialStateTransition {
                block_hash: child_block.hash,
                parent_block_hash,
                parent_state_root: parent_root,
                state_root: child_root,
            })
        );
        let account_before = provider
            .tx_ref()
            .cursor_dup_read::<tables::PartialStateAccountChangeSets>()
            .unwrap()
            .seek_by_key_subkey(1, address)
            .unwrap()
            .unwrap();
        assert_eq!(account_before.account, Some(Account::from(parent_account)));
        assert_eq!(account_before.storage_root, Some(parent_account.storage_root));
        assert_eq!(
            provider
                .tx_ref()
                .cursor_dup_read::<tables::PartialStateStorageChangeSets>()
                .unwrap()
                .seek_by_key_subkey(BlockNumberAddress((1, address)), slot_hash)
                .unwrap(),
            Some(StorageEntry::new(slot_hash, parent_value))
        );
        drop(provider);

        assert_eq!(
            factory.revert_partial_state_transition(child_block, &filter).unwrap(),
            PartialStateSnapPivot {
                block_number: 0,
                block_hash: parent_block_hash,
                state_root: parent_root,
            }
        );
        assert_eq!(factory.partial_state_root(&filter).unwrap(), parent_root);
        assert_eq!(
            factory.partial_state_checkpoint().unwrap(),
            Some(PartialStateCheckpoint {
                pivot: PartialStateSnapPivot {
                    block_number: 0,
                    block_hash: parent_block_hash,
                    state_root: parent_root,
                },
                filter_hash: filter.filter_hash(),
                status: PartialStateCheckpointStatus::Complete,
            })
        );

        let provider = factory.database_provider_ro().unwrap();
        assert_eq!(
            provider.tx_ref().get::<tables::PartialStateAccounts>(account_hash).unwrap(),
            Some(Account::from(parent_account))
        );
        assert_eq!(
            provider
                .tx_ref()
                .cursor_dup_read::<tables::PartialStateStorages>()
                .unwrap()
                .seek_by_key_subkey(account_hash, slot_hash)
                .unwrap(),
            Some(StorageEntry::new(slot_hash, parent_value))
        );
        assert_eq!(
            provider.tx_ref().get::<tables::PartialStateTransitionJournals>(1).unwrap(),
            None
        );
    }

    #[test]
    fn child_root_mismatch_rolls_back_transition() {
        let factory = create_test_provider_factory();
        let address = address!("0000000000000000000000000000000000000001");
        let account_hash = keccak256(address);
        let parent_account = TrieAccount {
            balance: U256::from(10),
            storage_root: EMPTY_ROOT_HASH,
            code_hash: KECCAK_EMPTY,
            ..Default::default()
        };
        let parent_root = state_root_unsorted([(account_hash, parent_account)]);
        let parent_pivot = PartialStateSnapPivot {
            block_number: 0,
            block_hash: B256::repeat_byte(0x10),
            state_root: parent_root,
        };
        let filter = ConfiguredContractFilter::new([address]);
        factory.begin_partial_state_sync(parent_pivot, &filter).unwrap();
        let provider = factory.database_provider_rw().unwrap();
        provider.partial_state_snap_writer().write_account(account_hash, parent_account).unwrap();
        provider.commit().unwrap();
        factory.complete_partial_state_sync(parent_pivot, &filter).unwrap();

        let access_list =
            vec![AccountChanges::new(address)
                .with_balance_change(BalanceChange::new(1, U256::from(11)))];
        let resolved_accounts = PartialStateResolvedAccounts::default();
        let err = factory
            .apply_partial_state_transition(
                PartialStateTransition {
                    block: BlockNumHash::new(1, B256::repeat_byte(0x11)),
                    parent_block_hash: B256::repeat_byte(0x10),
                    parent_root,
                    expected_root: B256::repeat_byte(0xff),
                    expected_bal_hash: compute_block_access_list_hash(&access_list),
                    access_list: &access_list,
                    resolved_accounts: &resolved_accounts,
                },
                &filter,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ProviderError::PartialStateTransition(error)
                if matches!(*error, PartialStateTransitionError::ChildRootMismatch { .. })
        ));
        assert_eq!(factory.partial_state_root(&filter).unwrap(), parent_root);
        assert_eq!(
            factory
                .database_provider_ro()
                .unwrap()
                .tx_ref()
                .get::<tables::PartialStateTransitionJournals>(1)
                .unwrap(),
            None
        );
        assert_eq!(
            factory
                .database_provider_ro()
                .unwrap()
                .tx_ref()
                .get::<tables::PartialStateAccounts>(account_hash)
                .unwrap(),
            Some(Account::from(parent_account))
        );
        assert_eq!(
            factory.partial_state_checkpoint().unwrap(),
            Some(PartialStateCheckpoint {
                pivot: parent_pivot,
                filter_hash: filter.filter_hash(),
                status: PartialStateCheckpointStatus::Complete,
            })
        );
    }

    #[test]
    fn untracked_storage_change_requires_resolved_commitment() {
        let factory = create_test_provider_factory();
        let address = address!("0000000000000000000000000000000000000002");
        let account_hash = keccak256(address);
        let slot = U256::from(1);
        let slot_hash = keccak256(slot.to_be_bytes::<32>());
        let parent_account = TrieAccount {
            nonce: 1,
            storage_root: storage_root([(slot_hash, U256::from(1))]),
            code_hash: KECCAK_EMPTY,
            ..Default::default()
        };
        let parent_root = state_root_unsorted([(account_hash, parent_account)]);
        let provider = factory.database_provider_rw().unwrap();
        provider.partial_state_snap_writer().write_account(account_hash, parent_account).unwrap();
        provider.commit().unwrap();

        let access_list = vec![AccountChanges::new(address).with_storage_change(SlotChanges::new(
            slot,
            vec![StorageChange::new(1, U256::from(2))],
        ))];
        let resolved_accounts = PartialStateResolvedAccounts::default();
        let filter = ConfiguredContractFilter::default();
        let err = factory
            .apply_partial_state_transition(
                PartialStateTransition {
                    block: BlockNumHash::new(1, B256::repeat_byte(0x11)),
                    parent_block_hash: B256::repeat_byte(0x10),
                    parent_root,
                    expected_root: B256::repeat_byte(0xee),
                    expected_bal_hash: compute_block_access_list_hash(&access_list),
                    access_list: &access_list,
                    resolved_accounts: &resolved_accounts,
                },
                &filter,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ProviderError::PartialStateTransition(error)
                if matches!(
                    *error,
                    PartialStateTransitionError::AccountCommitmentUnavailable {
                        address: missing,
                        ..
                    } if missing == address
                )
        ));
        assert_eq!(factory.partial_state_root(&filter).unwrap(), parent_root);
    }

    #[test]
    fn account_deletion_removes_retained_storage() {
        let factory = create_test_provider_factory();
        let address = address!("0000000000000000000000000000000000000002");
        let account_hash = keccak256(address);
        let slot = U256::from(1);
        let slot_hash = keccak256(slot.to_be_bytes::<32>());
        let slot_value = U256::from(10);
        let parent_account = TrieAccount {
            balance: U256::from(1),
            storage_root: storage_root([(slot_hash, slot_value)]),
            code_hash: KECCAK_EMPTY,
            ..Default::default()
        };
        let parent_root = state_root_unsorted([(account_hash, parent_account)]);
        let provider = factory.database_provider_rw().unwrap();
        {
            let mut writer = provider.partial_state_snap_writer();
            writer.write_account(account_hash, parent_account).unwrap();
            writer.write_storage(account_hash, slot_hash, slot_value).unwrap();
        }
        provider.commit().unwrap();

        let access_list = vec![AccountChanges::new(address)
            .with_storage_change(SlotChanges::new(slot, vec![StorageChange::new(1, U256::ZERO)]))
            .with_balance_change(BalanceChange::new(1, U256::ZERO))];
        let resolved_accounts = PartialStateResolvedAccounts::from([(address, None)]);
        let filter = ConfiguredContractFilter::default();
        let root = factory
            .apply_partial_state_transition(
                PartialStateTransition {
                    block: BlockNumHash::new(1, B256::repeat_byte(0x11)),
                    parent_block_hash: B256::repeat_byte(0x10),
                    parent_root,
                    expected_root: EMPTY_ROOT_HASH,
                    expected_bal_hash: compute_block_access_list_hash(&access_list),
                    access_list: &access_list,
                    resolved_accounts: &resolved_accounts,
                },
                &filter,
            )
            .unwrap();
        assert_eq!(root, EMPTY_ROOT_HASH);

        let provider = factory.database_provider_ro().unwrap();
        assert_eq!(
            provider.tx_ref().get::<tables::PartialStateAvailability>(account_hash).unwrap(),
            None
        );
        assert_eq!(
            provider.tx_ref().get::<tables::PartialStateAccounts>(account_hash).unwrap(),
            None
        );
        assert_eq!(
            provider.tx_ref().get::<tables::PartialStateStorageRoots>(account_hash).unwrap(),
            None
        );
        let mut storage =
            provider.tx_ref().cursor_dup_read::<tables::PartialStateStorages>().unwrap();
        assert_eq!(storage.seek_by_key_subkey(account_hash, B256::ZERO).unwrap(), None);
    }
}
