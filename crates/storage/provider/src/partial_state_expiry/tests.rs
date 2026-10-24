use super::*;
use crate::test_utils::{create_test_provider_factory, MockNodeTypesWithDB};
use alloy_consensus::{constants::KECCAK_EMPTY, Header};
use alloy_eip7928::{
    AccountChanges, BalanceChange, CodeChange, NonceChange, SlotChanges, StorageChange,
};
use alloy_eips::{eip7928::compute_block_access_list_hash, BlockNumHash};
use alloy_primitives::{address, Bytes, B256, U256};
use reth_db_api::{models::StoredPartialStateAvailability, table::Table};
use reth_primitives_traits::{Account, StorageEntry};
use reth_storage_api::{
    AccountReader, ConfiguredContractFilter, PartialStateCheckpointProvider,
    PartialStateResolvedAccounts, PartialStateSnapWriter, PartialStateTransition,
    PartialStateTransitionProvider,
};
use reth_trie::root::{state_root_unsorted, storage_root_unsorted};
use reth_trie_common::TrieAccount;
use std::collections::BTreeMap;

const EXPIRED: Address = address!("0000000000000000000000000000000000000001");
const RETAINED: Address = address!("0000000000000000000000000000000000000002");
const EMPTY: Address = address!("0000000000000000000000000000000000000003");
const ABSENT: Address = address!("0000000000000000000000000000000000000004");
const UNTRACKED: Address = address!("0000000000000000000000000000000000000005");
const CODE: &[u8] = &[0x60, 0x01, 0x00];

struct Fixture {
    factory: ProviderFactory<MockNodeTypesWithDB>,
    filter: ConfiguredContractFilter,
    pivot: PartialStateSnapPivot,
    accounts: BTreeMap<Address, TrieAccount>,
}

impl Fixture {
    fn new() -> Self {
        let factory = create_test_provider_factory();
        let filter = ConfiguredContractFilter::new([EXPIRED, RETAINED, EMPTY, ABSENT]);
        let values =
            [(EXPIRED, [10, 20]), (RETAINED, [30, 40]), (EMPTY, [0, 0]), (UNTRACKED, [50, 60])];
        let accounts: BTreeMap<_, _> = values
            .iter()
            .map(|(address, values)| {
                (
                    *address,
                    TrieAccount {
                        nonce: 1,
                        balance: U256::from(100),
                        storage_root: slots_root(*values),
                        code_hash: if *address == EMPTY { KECCAK_EMPTY } else { keccak256(CODE) },
                    },
                )
            })
            .collect();
        let pivot = PartialStateSnapPivot {
            block_number: 0,
            block_hash: B256::repeat_byte(0x10),
            state_root: accounts_root(&accounts),
        };
        factory.begin_partial_state_sync(pivot, &filter).unwrap();
        let provider = factory.database_provider_rw().unwrap();
        let mut writer = provider.partial_state_snap_writer();
        for (address, account) in &accounts {
            writer.write_account(keccak256(address), *account).unwrap();
        }
        for (address, values) in values {
            if !filter.should_sync_storage(&address) {
                continue
            }
            for (index, value) in values.into_iter().enumerate() {
                if value != 0 {
                    writer
                        .write_storage(
                            keccak256(address),
                            keccak256(slot(index)),
                            U256::from(value),
                        )
                        .unwrap();
                }
            }
        }
        writer.write_bytecode(keccak256(CODE), CODE).unwrap();
        // Normal full-state rows are deliberately present, so fallback or deletion is observable.
        let full_account = Account { balance: U256::from(999), ..Default::default() };
        provider.tx_ref().put::<tables::PlainAccountState>(EXPIRED, full_account).unwrap();
        provider.tx_ref().put::<tables::HashedAccounts>(keccak256(EXPIRED), full_account).unwrap();
        let entry = StorageEntry::new(slot(0), U256::from(999));
        provider.tx_ref().put::<tables::PlainStorageState>(EXPIRED, entry).unwrap();
        provider.tx_ref().put::<tables::HashedStorages>(keccak256(EXPIRED), entry).unwrap();
        provider.tx_ref().put::<tables::PartialStateHeaders<Header>>(0, Header::default()).unwrap();
        provider.commit().unwrap();
        factory.complete_partial_state_sync(pivot, &filter).unwrap();
        Self { factory, filter, pivot, accounts }
    }

    fn expire(&self, addresses: &[Address]) -> ProviderResult<PartialStateStorageExpiryOutcome> {
        self.factory.expire_partial_state_storage(self.pivot, &self.filter, addresses)
    }

    fn apply(&mut self, updates: &[(Address, [u64; 2])], resolve: bool) {
        let mut accounts = self.accounts.clone();
        let access_list: Vec<_> = updates
            .iter()
            .map(|(address, values)| {
                accounts.get_mut(address).unwrap().storage_root = slots_root(*values);
                AccountChanges::new(*address)
                    .with_storage_change(SlotChanges::new(
                        U256::from(1),
                        vec![StorageChange::new(1, U256::from(values[0]))],
                    ))
                    .with_storage_change(SlotChanges::new(
                        U256::from(2),
                        vec![StorageChange::new(1, U256::from(values[1]))],
                    ))
            })
            .collect();
        let resolved_accounts = if resolve {
            updates.iter().map(|(address, _)| (*address, Some(accounts[address]))).collect()
        } else {
            PartialStateResolvedAccounts::default()
        };
        let block = BlockNumHash::new(
            self.pivot.block_number + 1,
            B256::repeat_byte(0x11 + self.pivot.block_number as u8),
        );
        let state_root = accounts_root(&accounts);
        assert_eq!(
            self.factory
                .apply_partial_state_transition(
                    PartialStateTransition {
                        block,
                        parent_block_hash: self.pivot.block_hash,
                        parent_root: self.pivot.state_root,
                        expected_root: state_root,
                        expected_bal_hash: compute_block_access_list_hash(&access_list),
                        access_list: &access_list,
                        resolved_accounts: &resolved_accounts,
                    },
                    &self.filter
                )
                .unwrap(),
            state_root
        );
        self.pivot = PartialStateSnapPivot {
            block_number: block.number,
            block_hash: block.hash,
            state_root,
        };
        self.accounts = accounts;
    }

    fn assert_unavailable(&self) {
        let reader = self.factory.partial_state_reader(self.pivot, &self.filter).unwrap();
        for key in [slot(0), slot(1), B256::repeat_byte(99)] {
            assert!(
                matches!(reader.storage(EXPIRED, key), Err(ProviderError::PartialStateRead(error))
                if matches!(*error, PartialStateReadError::StorageUnavailable(EXPIRED)))
            );
        }
        assert!(!reader.state_availability(&EXPIRED).unwrap().storage);
        assert!(reader.state_availability(&EXPIRED).unwrap().bytecode);
        assert_eq!(reader.account_code(&EXPIRED).unwrap().unwrap().original_bytes().as_ref(), CODE);
        assert_eq!(self.factory.partial_state_root(&self.filter).unwrap(), self.pivot.state_root);
        assert_eq!(self.factory.partial_state_checkpoint().unwrap().unwrap().pivot, self.pivot);
        assert!(rows::<tables::PartialStateStorages>(&self.factory)
            .iter()
            .all(|(hash, _)| *hash != keccak256(EXPIRED)));
    }
}

fn slot(index: usize) -> B256 {
    B256::from(U256::from(index + 1).to_be_bytes::<32>())
}

fn slots_root(values: [u64; 2]) -> B256 {
    storage_root_unsorted(
        values
            .into_iter()
            .enumerate()
            .filter(|(_, value)| *value != 0)
            .map(|(index, value)| (keccak256(slot(index)), U256::from(value))),
    )
}

fn accounts_root(accounts: &BTreeMap<Address, TrieAccount>) -> B256 {
    state_root_unsorted(accounts.iter().map(|(address, account)| (keccak256(address), *account)))
}

fn rows<T: Table>(factory: &ProviderFactory<MockNodeTypesWithDB>) -> Vec<(T::Key, T::Value)> {
    factory
        .database_provider_ro()
        .unwrap()
        .tx_ref()
        .cursor_read::<T>()
        .unwrap()
        .walk(None)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn snapshot(factory: &ProviderFactory<MockNodeTypesWithDB>) -> Vec<String> {
    macro_rules! tables {
        ($($table:ty),* $(,)?) => { vec![$(format!("{:?}", rows::<$table>(factory))),*] };
    }
    tables![
        tables::PartialStateAccounts,
        tables::PartialStateStorageRoots,
        tables::PartialStateStorages,
        tables::PartialStateAvailability,
        tables::PartialStateAccountChangeSets,
        tables::PartialStateStorageChangeSets,
        tables::PartialStateAvailabilityChangeSets,
        tables::PartialStateTransitionJournals,
        tables::PartialStateCheckpoints,
        tables::PartialStateHeaders<Header>,
        tables::Bytecodes,
        tables::PlainAccountState,
        tables::PlainStorageState,
        tables::HashedAccounts,
        tables::HashedStorages,
        tables::StageCheckpoints,
    ]
}

#[test]
fn expiry_deletes_only_selected_storage_and_is_idempotent() {
    let fixture = Fixture::new();
    let before = snapshot(&fixture.factory);
    let outcome = fixture.expire(&[EXPIRED, EXPIRED]).unwrap();
    assert_eq!(
        outcome,
        PartialStateStorageExpiryOutcome {
            accounts_expired: 1,
            slots_deleted: 2,
            journal_slots_deleted: 0
        }
    );
    fixture.assert_unavailable();
    let after = snapshot(&fixture.factory);
    for (index, (before, after)) in before.iter().zip(&after).enumerate() {
        if index != 2 && index != 3 {
            assert_eq!(before, after, "unexpected change to table {index}");
        }
    }
    let reader = fixture.factory.partial_state_reader(fixture.pivot, &fixture.filter).unwrap();
    assert_eq!(reader.storage(RETAINED, slot(0)).unwrap(), Some(U256::from(30)));
    assert_eq!(reader.storage(RETAINED, slot(1)).unwrap(), Some(U256::from(40)));
    assert_eq!(
        reader.basic_account(&EXPIRED).unwrap(),
        Some(Account::from(fixture.accounts[&EXPIRED]))
    );
    assert_eq!(
        reader.account_storage_root(&EXPIRED).unwrap(),
        Some(fixture.accounts[&EXPIRED].storage_root)
    );
    assert_eq!(fixture.expire(&[EXPIRED]).unwrap(), PartialStateStorageExpiryOutcome::default());
    assert_eq!(fixture.expire(&[]).unwrap(), PartialStateStorageExpiryOutcome::default());
    assert_eq!(snapshot(&fixture.factory), after);
    fixture.factory.complete_partial_state_sync(fixture.pivot, &fixture.filter).unwrap();
    fixture.assert_unavailable();
}

#[test]
fn expiry_preserves_existing_reader_snapshot_and_known_empty_state() {
    let fixture = Fixture::new();
    let old = fixture.factory.partial_state_reader(fixture.pivot, &fixture.filter).unwrap();
    fixture.expire(&[EXPIRED, EMPTY, ABSENT]).unwrap();
    assert_eq!(old.storage(EXPIRED, slot(0)).unwrap(), Some(U256::from(10)));
    assert!(old.state_availability(&EXPIRED).unwrap().storage);
    fixture.assert_unavailable();
    let reader = fixture.factory.partial_state_reader(fixture.pivot, &fixture.filter).unwrap();
    for address in [EMPTY, ABSENT] {
        assert_eq!(reader.storage(address, slot(0)).unwrap(), None);
        assert!(reader.state_availability(&address).unwrap().storage);
    }
}

#[test]
fn expiry_rejects_stale_checkpoint_filter_and_untracked_targets() {
    let fixture = Fixture::new();
    let before = snapshot(&fixture.factory);
    for pivot in [
        PartialStateSnapPivot { block_number: 1, ..fixture.pivot },
        PartialStateSnapPivot { block_hash: B256::ZERO, ..fixture.pivot },
        PartialStateSnapPivot { state_root: B256::ZERO, ..fixture.pivot },
    ] {
        assert!(
            matches!(fixture.factory.expire_partial_state_storage(pivot, &fixture.filter, &[EXPIRED]),
            Err(ProviderError::PartialStateCheckpoint(error)) if matches!(*error, PartialStateCheckpointError::PivotMismatch { .. }))
        );
    }
    let wrong_filter = ConfiguredContractFilter::new([EXPIRED]);
    assert!(
        matches!(fixture.factory.expire_partial_state_storage(fixture.pivot, &wrong_filter, &[EXPIRED]),
        Err(ProviderError::PartialStateCheckpoint(error)) if matches!(*error, PartialStateCheckpointError::FilterMismatch { .. }))
    );
    assert!(matches!(
        fixture.expire(&[EXPIRED, UNTRACKED]),
        Err(ProviderError::StorageNotTracked(UNTRACKED))
    ));
    assert_eq!(snapshot(&fixture.factory), before);
}

#[test]
fn expiry_rejects_missing_and_incomplete_checkpoints() {
    let fixture = Fixture::new();
    fixture.factory.begin_partial_state_sync(fixture.pivot, &fixture.filter).unwrap();
    let before = snapshot(&fixture.factory);
    assert!(matches!(fixture.expire(&[EXPIRED]), Err(ProviderError::PartialStateCheckpoint(error))
        if matches!(*error, PartialStateCheckpointError::Incomplete { .. })));
    assert_eq!(snapshot(&fixture.factory), before);
    fixture.factory.reset_partial_state().unwrap();
    let before = snapshot(&fixture.factory);
    assert!(matches!(fixture.expire(&[EXPIRED]), Err(ProviderError::PartialStateCheckpoint(error))
        if matches!(*error, PartialStateCheckpointError::Unavailable)));
    assert_eq!(snapshot(&fixture.factory), before);
}

#[test]
fn expiry_rejects_corrupt_state_before_deletion() {
    let fixture = Fixture::new();
    let provider = fixture.factory.database_provider_rw().unwrap();
    provider.tx_ref().delete::<tables::PartialStateStorages>(keccak256(EXPIRED), None).unwrap();
    provider.commit().unwrap();
    let before = snapshot(&fixture.factory);
    assert!(
        matches!(fixture.expire(&[EXPIRED, RETAINED]), Err(ProviderError::PartialStateCheckpoint(error))
        if matches!(*error, PartialStateCheckpointError::RootMismatch { .. }))
    );
    assert_eq!(snapshot(&fixture.factory), before);
}

#[test]
fn expiry_rolls_back_deletion_when_preserved_commitment_is_wrong() {
    let fixture = Fixture::new();
    let provider = fixture.factory.database_provider_rw().unwrap();
    provider
        .tx_ref()
        .put::<tables::PartialStateStorageRoots>(keccak256(EXPIRED), B256::repeat_byte(0xff))
        .unwrap();
    provider.commit().unwrap();
    // Available slots still compute the correct root before expiry. Switching to the corrupt
    // commitment must fail the post-deletion check and roll back every deletion/metadata write.
    assert_eq!(
        fixture.factory.partial_state_root(&fixture.filter).unwrap(),
        fixture.pivot.state_root
    );
    let before = snapshot(&fixture.factory);
    assert!(
        matches!(fixture.expire(&[EXPIRED, RETAINED]), Err(ProviderError::PartialStateCheckpoint(error))
        if matches!(*error, PartialStateCheckpointError::RootMismatch { .. }))
    );
    assert_eq!(snapshot(&fixture.factory), before);
}

#[test]
fn expiry_rolls_back_batch_when_journal_availability_is_missing() {
    let mut fixture = Fixture::new();
    fixture.apply(&[(EXPIRED, [11, 21]), (RETAINED, [31, 41])], false);
    let provider = fixture.factory.database_provider_rw().unwrap();
    provider
        .tx_ref()
        .delete::<tables::PartialStateAvailabilityChangeSets>(
            BlockNumberAddress((1, RETAINED)),
            None,
        )
        .unwrap();
    provider.commit().unwrap();
    let before = snapshot(&fixture.factory);
    assert!(
        matches!(fixture.expire(&[EXPIRED, RETAINED]), Err(ProviderError::PartialStateRead(error))
        if matches!(*error, PartialStateReadError::MissingAvailability(hash) if hash == keccak256(RETAINED)))
    );
    assert_eq!(snapshot(&fixture.factory), before);
}

#[test]
fn expiry_removes_journal_slots_without_breaking_multi_block_reorgs() {
    let mut fixture = Fixture::new();
    let parent = fixture.pivot;
    let parent_accounts = fixture.accounts.clone();
    fixture.apply(&[(EXPIRED, [11, 21]), (RETAINED, [31, 41])], false);
    let child = fixture.pivot;
    fixture.apply(&[(EXPIRED, [12, 22]), (RETAINED, [32, 42])], false);
    let account_journals = rows::<tables::PartialStateAccountChangeSets>(&fixture.factory);
    let block_journals = rows::<tables::PartialStateTransitionJournals>(&fixture.factory);
    assert_eq!(
        fixture.expire(&[EXPIRED]).unwrap(),
        PartialStateStorageExpiryOutcome {
            accounts_expired: 1,
            slots_deleted: 2,
            journal_slots_deleted: 4,
        }
    );
    assert_eq!(rows::<tables::PartialStateAccountChangeSets>(&fixture.factory), account_journals);
    assert_eq!(rows::<tables::PartialStateTransitionJournals>(&fixture.factory), block_journals);
    assert!(rows::<tables::PartialStateStorageChangeSets>(&fixture.factory)
        .iter()
        .all(|(key, _)| key.address() == RETAINED));
    for expected in [child, parent] {
        fixture.pivot = fixture
            .factory
            .revert_partial_state_transition(
                BlockNumHash::new(fixture.pivot.block_number, fixture.pivot.block_hash),
                &fixture.filter,
            )
            .unwrap();
        assert_eq!(fixture.pivot, expected);
        fixture.assert_unavailable();
    }
    let reader = fixture.factory.partial_state_reader(parent, &fixture.filter).unwrap();
    assert_eq!(reader.storage(RETAINED, slot(0)).unwrap(), Some(U256::from(30)));
    assert_eq!(reader.storage(RETAINED, slot(1)).unwrap(), Some(U256::from(40)));
    fixture.accounts = parent_accounts;
    fixture.apply(&[(EXPIRED, [99, 20]), (RETAINED, [33, 40])], true);
    fixture.assert_unavailable();
}

#[test]
fn journals_distinct_slots_in_descending_hash_order() {
    let mut fixture = Fixture::new();
    let parent = fixture.pivot;
    assert!(keccak256(slot(1)) < keccak256(slot(0)));
    fixture.apply(&[(RETAINED, [31, 41])], false);
    assert_eq!(rows::<tables::PartialStateStorageChangeSets>(&fixture.factory).len(), 2);
    let restored = fixture
        .factory
        .revert_partial_state_transition(
            BlockNumHash::new(fixture.pivot.block_number, fixture.pivot.block_hash),
            &fixture.filter,
        )
        .unwrap();
    assert_eq!(restored, parent);
    let reader = fixture.factory.partial_state_reader(restored, &fixture.filter).unwrap();
    assert_eq!(reader.storage(RETAINED, slot(0)).unwrap(), Some(U256::from(30)));
    assert_eq!(reader.storage(RETAINED, slot(1)).unwrap(), Some(U256::from(40)));
}

#[test]
fn expiry_survives_peer_resolved_advancement_and_rollback() {
    let mut fixture = Fixture::new();
    fixture.expire(&[EXPIRED]).unwrap();
    let parent = fixture.pivot;
    fixture.apply(&[(EXPIRED, [11, 20])], true);
    fixture.assert_unavailable();
    assert!(rows::<tables::PartialStateStorageChangeSets>(&fixture.factory).is_empty());
    fixture.pivot = fixture
        .factory
        .revert_partial_state_transition(
            BlockNumHash::new(fixture.pivot.block_number, fixture.pivot.block_hash),
            &fixture.filter,
        )
        .unwrap();
    assert_eq!(fixture.pivot, parent);
    fixture.assert_unavailable();
}

#[test]
fn expiry_of_currently_empty_storage_removes_nonempty_history() {
    let mut fixture = Fixture::new();
    let parent = fixture.pivot;
    fixture.apply(&[(EXPIRED, [0, 0])], false);
    assert_eq!(
        fixture.expire(&[EXPIRED]).unwrap(),
        PartialStateStorageExpiryOutcome {
            accounts_expired: 0,
            slots_deleted: 0,
            journal_slots_deleted: 2,
        }
    );
    let reader = fixture.factory.partial_state_reader(fixture.pivot, &fixture.filter).unwrap();
    assert_eq!(reader.storage(EXPIRED, slot(0)).unwrap(), None);
    fixture.pivot = fixture
        .factory
        .revert_partial_state_transition(
            BlockNumHash::new(fixture.pivot.block_number, fixture.pivot.block_hash),
            &fixture.filter,
        )
        .unwrap();
    assert_eq!(fixture.pivot, parent);
    fixture.assert_unavailable();
}

#[test]
fn expiry_allows_complete_storage_to_be_rebuilt_from_known_empty_state() {
    let mut fixture = Fixture::new();
    fixture.expire(&[EXPIRED]).unwrap();
    fixture.apply(&[(EXPIRED, [0, 0])], true);
    let empty = fixture.pivot;
    fixture.apply(&[(EXPIRED, [7, 8])], false);
    let reader = fixture.factory.partial_state_reader(fixture.pivot, &fixture.filter).unwrap();
    assert!(reader.state_availability(&EXPIRED).unwrap().storage);
    assert_eq!(reader.storage(EXPIRED, slot(0)).unwrap(), Some(U256::from(7)));
    assert_eq!(reader.storage(EXPIRED, slot(1)).unwrap(), Some(U256::from(8)));
    fixture.pivot = fixture
        .factory
        .revert_partial_state_transition(
            BlockNumHash::new(fixture.pivot.block_number, fixture.pivot.block_hash),
            &fixture.filter,
        )
        .unwrap();
    assert_eq!(fixture.pivot, empty);
    fixture.pivot = fixture
        .factory
        .revert_partial_state_transition(
            BlockNumHash::new(fixture.pivot.block_number, fixture.pivot.block_hash),
            &fixture.filter,
        )
        .unwrap();
    fixture.assert_unavailable();
}

#[test]
fn expiry_remains_unavailable_after_journal_pruning() {
    let mut fixture = Fixture::new();
    fixture.apply(&[(EXPIRED, [11, 20])], false);
    fixture.expire(&[EXPIRED]).unwrap();
    assert_eq!(fixture.factory.prune_partial_state_transition_journal(2).unwrap(), 1);
    fixture.assert_unavailable();
    assert!(rows::<tables::PartialStateAvailabilityChangeSets>(&fixture.factory).is_empty());
    assert!(rows::<tables::PartialStateStorageChangeSets>(&fixture.factory).is_empty());
    fixture.factory.reset_partial_state().unwrap();
    assert!(rows::<tables::PartialStateAvailability>(&fixture.factory).is_empty());
}

#[test]
fn expiry_removes_stale_rows_for_already_unavailable_storage() {
    let fixture = Fixture::new();
    let provider = fixture.factory.database_provider_rw().unwrap();
    provider
        .tx_ref()
        .put::<tables::PartialStateAvailability>(
            keccak256(EXPIRED),
            StoredPartialStateAvailability { storage: false, bytecode: true },
        )
        .unwrap();
    provider.commit().unwrap();
    assert_eq!(
        fixture.expire(&[EXPIRED]).unwrap(),
        PartialStateStorageExpiryOutcome {
            accounts_expired: 0,
            slots_deleted: 2,
            journal_slots_deleted: 0,
        }
    );
    fixture.assert_unavailable();
}

#[test]
fn expiry_requires_resolved_commitments_for_further_storage_changes() {
    let mut fixture = Fixture::new();
    fixture.expire(&[EXPIRED]).unwrap();
    let before = snapshot(&fixture.factory);
    let access_list = vec![AccountChanges::new(EXPIRED).with_storage_change(SlotChanges::new(
        U256::from(1),
        vec![StorageChange::new(1, U256::from(11))],
    ))];
    let mut child_accounts = fixture.accounts.clone();
    child_accounts.get_mut(&EXPIRED).unwrap().storage_root = slots_root([11, 20]);
    let mut wrong = child_accounts[&EXPIRED];
    wrong.storage_root = slots_root([999, 20]);
    for resolved in
        [PartialStateResolvedAccounts::default(), BTreeMap::from([(EXPIRED, Some(wrong))])]
    {
        assert!(fixture
            .factory
            .apply_partial_state_transition(
                PartialStateTransition {
                    block: BlockNumHash::new(1, B256::repeat_byte(0x11)),
                    parent_block_hash: fixture.pivot.block_hash,
                    parent_root: fixture.pivot.state_root,
                    expected_root: accounts_root(&child_accounts),
                    expected_bal_hash: compute_block_access_list_hash(&access_list),
                    access_list: &access_list,
                    resolved_accounts: &resolved,
                },
                &fixture.filter
            )
            .is_err());
        assert_eq!(snapshot(&fixture.factory), before);
    }
    fixture.apply(&[(EXPIRED, [11, 20])], true);
    fixture.assert_unavailable();
}

#[test]
fn expiry_of_deleted_account_does_not_restore_storage_on_reorg() {
    let mut fixture = Fixture::new();
    let parent = fixture.pivot;
    let access_list = vec![AccountChanges::new(EXPIRED)
        .with_balance_change(BalanceChange::new(1, U256::ZERO))
        .with_nonce_change(NonceChange::new(1, 0))
        .with_code_change(CodeChange::new(1, Bytes::new()))
        .with_storage_change(SlotChanges::new(
            U256::from(1),
            vec![StorageChange::new(1, U256::ZERO)],
        ))
        .with_storage_change(SlotChanges::new(
            U256::from(2),
            vec![StorageChange::new(1, U256::ZERO)],
        ))];
    fixture.accounts.remove(&EXPIRED);
    let child = PartialStateSnapPivot {
        block_number: 1,
        block_hash: B256::repeat_byte(0x11),
        state_root: accounts_root(&fixture.accounts),
    };
    fixture
        .factory
        .apply_partial_state_transition(
            PartialStateTransition {
                block: BlockNumHash::new(1, child.block_hash),
                parent_block_hash: parent.block_hash,
                parent_root: parent.state_root,
                expected_root: child.state_root,
                expected_bal_hash: compute_block_access_list_hash(&access_list),
                access_list: &access_list,
                resolved_accounts: &PartialStateResolvedAccounts::default(),
            },
            &fixture.filter,
        )
        .unwrap();
    fixture.pivot = child;
    let reader = fixture.factory.partial_state_reader(child, &fixture.filter).unwrap();
    assert_eq!(reader.basic_account(&EXPIRED).unwrap(), None);
    assert_eq!(
        fixture.expire(&[EXPIRED]).unwrap(),
        PartialStateStorageExpiryOutcome {
            accounts_expired: 0,
            slots_deleted: 0,
            journal_slots_deleted: 2,
        }
    );
    fixture.pivot = fixture
        .factory
        .revert_partial_state_transition(BlockNumHash::new(1, child.block_hash), &fixture.filter)
        .unwrap();
    assert_eq!(fixture.pivot, parent);
    fixture.assert_unavailable();
}
