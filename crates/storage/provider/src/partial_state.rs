use crate::{providers::ProviderNodeTypes, DatabaseProvider, ProviderFactory};
use alloy_consensus::constants::{EMPTY_ROOT_HASH, KECCAK_EMPTY};
use alloy_eips::{
    eip7928::{compute_block_access_list_hash, AccountChanges},
    BlockNumHash,
};
use alloy_primitives::{keccak256, Address, BlockNumber, Bytes, B256};
use reth_db_api::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    models::{BlockNumberAddress, PartialStateAccountBefore, StoredPartialStateTransition},
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::{Account, Bytecode, StorageEntry};
use reth_storage_api::{
    ContractFilter, DBProvider, DatabaseProviderFactory, PartialStateRootProvider,
    PartialStateSnapPivot, PartialStateTransition, PartialStateTransitionProvider,
};
use reth_storage_errors::provider::{PartialStateTransitionError, ProviderError, ProviderResult};
use reth_trie_common::TrieAccount;

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

fn apply_partial_state_transition<TX, N>(
    provider: &DatabaseProvider<TX, N>,
    transition: PartialStateTransition<'_>,
    filter: &dyn ContractFilter,
) -> ProviderResult<B256>
where
    TX: DbTx + DbTxMut + Send + Sync + 'static,
    N: ProviderNodeTypes,
{
    let current_root = provider.partial_state_root(filter)?;
    if current_root != transition.parent_root {
        return Err(PartialStateTransitionError::ParentRootMismatch {
            expected: transition.parent_root,
            computed: current_root,
        }
        .into())
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
    Ok(computed_root)
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
    }

    let storage_root = if has_storage_changes {
        if filter.should_sync_storage(&address) {
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
    } else if filter.should_sync_storage(&address) {
        provider.partial_storage_root_by_hash(account_hash)?
    } else {
        provider
            .tx_ref()
            .get::<tables::PartialStateStorageRoots>(account_hash)?
            .unwrap_or(EMPTY_ROOT_HASH)
    };

    if account.is_empty() && storage_root == EMPTY_ROOT_HASH {
        delete_account(provider, account_hash)?;
    } else {
        provider.tx_ref().put::<tables::PartialStateAccounts>(account_hash, account)?;
        write_storage_commitment(provider, account_hash, storage_root)?;
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

    journal.upsert(
        block_number,
        &PartialStateAccountBefore {
            address,
            account: provider.tx_ref().get::<tables::PartialStateAccounts>(account_hash)?,
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
        if journal.seek_by_key_subkey(journal_key, slot_hash)?.is_none() {
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
        } else {
            provider.tx_ref().delete::<tables::PartialStateAccounts>(account_hash, None)?;
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

    Ok(PartialStateSnapPivot {
        block_number: parent_number,
        block_hash: journal.parent_block_hash,
        state_root: journal.parent_state_root,
    })
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
    // Bytecodes are content-addressed and shared with canonical state, so old entries are harmless.
    provider.tx_ref().clear::<tables::PartialStateStorageChangeSets>()?;
    provider.tx_ref().clear::<tables::PartialStateAccountChangeSets>()?;
    provider.tx_ref().clear::<tables::PartialStateTransitionJournals>()?;
    provider.tx_ref().clear::<tables::PartialStateStorages>()?;
    provider.tx_ref().clear::<tables::PartialStateStorageRoots>()?;
    provider.tx_ref().clear::<tables::PartialStateAccounts>()?;
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
    use alloy_eip7928::{BalanceChange, CodeChange, NonceChange, SlotChanges, StorageChange};
    use alloy_eips::eip7928::{compute_block_access_list_hash, BlockAccessList};
    use alloy_primitives::{address, U256};
    use reth_db_api::{cursor::DbDupCursorRO, transaction::DbTx};
    use reth_storage_api::{
        ConfiguredContractFilter, DBProvider, PartialStateResolvedAccounts, PartialStateSnapWriter,
    };
    use reth_trie::root::{state_root_unsorted, storage_root};

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
        let provider = factory.database_provider_rw().unwrap();
        provider.partial_state_snap_writer().write_account(account_hash, parent_account).unwrap();
        provider.commit().unwrap();

        let access_list =
            vec![AccountChanges::new(address)
                .with_balance_change(BalanceChange::new(1, U256::from(11)))];
        let resolved_accounts = PartialStateResolvedAccounts::default();
        let filter = ConfiguredContractFilter::new([address]);
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
