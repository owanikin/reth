use crate::{providers::ProviderNodeTypes, DatabaseProvider, ProviderFactory};
use alloy_consensus::constants::{EMPTY_ROOT_HASH, KECCAK_EMPTY};
use alloy_eips::eip7928::{compute_block_access_list_hash, AccountChanges};
use alloy_primitives::{keccak256, Bytes, B256};
use reth_db_api::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::{Account, Bytecode, StorageEntry};
use reth_storage_api::{
    ContractFilter, DBProvider, DatabaseProviderFactory, PartialStateRootProvider,
    PartialStateTransition, PartialStateTransitionProvider,
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
            apply_tracked_storage_changes(provider, account_hash, changes)?;
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

fn apply_tracked_storage_changes<TX, N>(
    provider: &DatabaseProvider<TX, N>,
    account_hash: B256,
    changes: &AccountChanges,
) -> ProviderResult<()>
where
    TX: DbTx + DbTxMut + 'static,
    N: ProviderNodeTypes,
{
    let mut cursor = provider.tx_ref().cursor_dup_write::<tables::PartialStateStorages>()?;
    for slot_changes in &changes.storage_changes {
        let Some(change) =
            slot_changes.changes.iter().max_by_key(|change| change.block_access_index)
        else {
            continue
        };
        let slot_hash = keccak256(slot_changes.slot.to_be_bytes::<32>());
        if cursor
            .seek_by_key_subkey(account_hash, slot_hash)?
            .is_some_and(|entry| entry.key == slot_hash)
        {
            cursor.delete_current()?;
        }
        if !change.new_value.is_zero() {
            cursor.upsert(account_hash, &StorageEntry::new(slot_hash, change.new_value))?;
        }
    }
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
