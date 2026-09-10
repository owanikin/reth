use crate::{
    partial_state::{ensure_complete_checkpoint, read_partial_state_checkpoint},
    providers::ProviderNodeTypes,
    DatabaseProviderRO, ProviderFactory,
};
use alloy_consensus::constants::{EMPTY_ROOT_HASH, KECCAK_EMPTY};
use alloy_primitives::{keccak256, Address, BlockNumber, Bytes, StorageKey, StorageValue, B256};
use reth_db_api::{cursor::DbDupCursorRO, tables, transaction::DbTx};
use reth_primitives_traits::{Account, Bytecode};
use reth_storage_api::{
    AccountReader, BlockHashReader, BytecodeReader, ConfiguredContractFilter, ContractFilter,
    DatabaseProviderFactory, HashedPostStateProvider, PartialStateCheckpoint,
    PartialStateSnapPivot, StateProofProvider, StateProvider, StateRootProvider,
    StorageRootProvider,
};
use reth_storage_errors::provider::{
    PartialStateCheckpointError, PartialStateReadError, ProviderError, ProviderResult,
};
use reth_trie_common::{
    updates::TrieUpdates, AccountProof, ExecutionWitnessMode, HashedPostState, HashedStorage,
    KeccakKeyHasher, MultiProof, MultiProofTargets, StorageMultiProof, StorageProof, TrieInput,
};
use revm_database::BundleState;
use std::collections::{BTreeMap, BTreeSet};

/// Reads accounts, storage, and bytecode at a completed partial-state checkpoint.
///
/// The checkpoint and all reads share one read-only database transaction. Existing readers retain
/// their snapshot across BAL commits, reorgs, and sync resets. Keep readers short-lived so they do
/// not retain old database pages unnecessarily.
///
/// Account and storage reads never fall back to full-state tables. The content-addressed bytecode
/// table is shared with normal execution, but only code hashes referenced by tracked accounts in
/// this snapshot are readable. This reader does not provide historical state or trie proofs, and
/// does not select or validate the caller's canonical head.
#[derive(Debug)]
pub struct PartialStateReader<N: ProviderNodeTypes> {
    provider: DatabaseProviderRO<N::DB, N>,
    checkpoint: PartialStateCheckpoint,
    filter: ConfiguredContractFilter,
    tracked_code_hashes: BTreeSet<B256>,
    /// Recent canonical hashes captured by the factory, never read from a moving head.
    block_hashes: BTreeMap<BlockNumber, B256>,
}

impl<N: ProviderNodeTypes> PartialStateReader<N> {
    fn new(
        provider: DatabaseProviderRO<N::DB, N>,
        expected: PartialStateSnapPivot,
        filter: ConfiguredContractFilter,
    ) -> ProviderResult<Self> {
        let checkpoint = read_partial_state_checkpoint(&provider)?
            .ok_or(PartialStateCheckpointError::Unavailable)?;
        ensure_complete_checkpoint(checkpoint, expected, &filter)?;

        // Hash-only bytecode lookups carry no address, so derive their allowed hashes from the
        // same verified snapshot instead of trusting whatever the shared code table contains.
        let mut tracked_code_hashes = BTreeSet::new();
        for hash in filter.contract_hashes() {
            if let Some(account) = provider.tx_ref().get::<tables::PartialStateAccounts>(*hash)? &&
                let Some(code_hash) = account.bytecode_hash &&
                code_hash != KECCAK_EMPTY
            {
                tracked_code_hashes.insert(code_hash);
            }
        }

        Ok(Self {
            provider,
            checkpoint,
            filter,
            tracked_code_hashes,
            block_hashes: BTreeMap::from([(expected.block_number, expected.block_hash)]),
        })
    }

    pub(crate) fn with_block_hashes(
        mut self,
        hashes: impl IntoIterator<Item = (BlockNumber, B256)>,
    ) -> Self {
        self.block_hashes.extend(hashes);
        self
    }

    /// Returns the verified block, root, and filter identity represented by this snapshot.
    pub const fn checkpoint(&self) -> PartialStateCheckpoint {
        self.checkpoint
    }

    /// Returns the account's storage-root commitment, including for untracked accounts.
    ///
    /// Returns `None` only if the account does not exist. A non-empty commitment does not imply
    /// that storage slots are available locally.
    pub fn account_storage_root(&self, address: &Address) -> ProviderResult<Option<B256>> {
        let account_hash = keccak256(address);
        if self.provider.tx_ref().get::<tables::PartialStateAccounts>(account_hash)?.is_none() {
            return Ok(None)
        }
        Ok(Some(
            self.provider
                .tx_ref()
                .get::<tables::PartialStateStorageRoots>(account_hash)?
                .unwrap_or(EMPTY_ROOT_HASH),
        ))
    }

    /// Reads a slot using the unhashed address and 32-byte storage key.
    ///
    /// `None` means a known-zero slot: the account or storage trie is absent, or the slot is
    /// absent from a tracked account's complete storage. Non-empty untracked storage returns
    /// [`ProviderError::StorageNotTracked`] even if other tables contain the requested slot.
    pub fn storage(
        &self,
        address: Address,
        storage_key: StorageKey,
    ) -> ProviderResult<Option<StorageValue>> {
        match self.account_storage_root(&address)? {
            None | Some(EMPTY_ROOT_HASH) => return Ok(None),
            _ => {}
        }
        if !self.filter.should_sync_storage(&address) {
            return Err(ProviderError::StorageNotTracked(address))
        }

        let slot_hash = keccak256(storage_key);
        let mut cursor =
            self.provider.tx_ref().cursor_dup_read::<tables::PartialStateStorages>()?;
        Ok(cursor
            .seek_by_key_subkey(keccak256(address), slot_hash)?
            .filter(|entry| entry.key == slot_hash)
            .map(|entry| entry.value))
    }

    /// Reads bytecode by address, enforcing tracking even when an untracked account shares code
    /// with a tracked account. Returns `None` for a nonexistent account or a known-empty code hash.
    pub fn account_code(&self, address: &Address) -> ProviderResult<Option<Bytecode>> {
        let Some(account) = self.basic_account(address)? else { return Ok(None) };
        let code_hash = account.get_bytecode_hash();
        if code_hash == KECCAK_EMPTY {
            return Ok(None)
        }
        if !self.filter.should_sync_code(address) {
            return Err(ProviderError::CodeNotTracked(*address))
        }
        self.bytecode_by_hash(&code_hash)
    }
}

impl<N: ProviderNodeTypes> AccountReader for PartialStateReader<N> {
    /// Reads account metadata for any address, whether or not its storage or code is tracked.
    fn basic_account(&self, address: &Address) -> ProviderResult<Option<Account>> {
        self.provider
            .tx_ref()
            .get::<tables::PartialStateAccounts>(keccak256(address))
            .map_err(Into::into)
    }
}

impl<N: ProviderNodeTypes> BytecodeReader for PartialStateReader<N> {
    /// Reads only code referenced by tracked accounts at this checkpoint. Missing or corrupt
    /// code returns an error; only the known-empty code hash returns `None`.
    fn bytecode_by_hash(&self, code_hash: &B256) -> ProviderResult<Option<Bytecode>> {
        if *code_hash == KECCAK_EMPTY {
            return Ok(None)
        }
        if !self.tracked_code_hashes.contains(code_hash) {
            return Err(PartialStateReadError::CodeHashNotTracked(*code_hash).into())
        }
        let code = self
            .provider
            .tx_ref()
            .get::<tables::Bytecodes>(*code_hash)?
            .ok_or(PartialStateReadError::MissingBytecode(*code_hash))?;
        let computed = keccak256(code.original_bytes());
        if computed != *code_hash {
            return Err(PartialStateReadError::BytecodeHashMismatch {
                expected: *code_hash,
                computed,
            }
            .into())
        }
        Ok(Some(code))
    }
}

impl<N: ProviderNodeTypes> ProviderFactory<N> {
    /// Opens a partial-state snapshot only if its complete checkpoint matches `expected` and
    /// `filter`. The caller must select the desired block explicitly; a lagging checkpoint is not
    /// silently substituted for the requested state. Only the checkpoint's own block hash is
    /// available here; [`crate::providers::BlockchainProvider`] also captures its recent canonical
    /// hashes.
    pub fn partial_state_reader(
        &self,
        expected: PartialStateSnapPivot,
        filter: &ConfiguredContractFilter,
    ) -> ProviderResult<PartialStateReader<N>> {
        PartialStateReader::new(self.database_provider_ro()?, expected, filter.clone())
    }
}

impl<N: ProviderNodeTypes> StateProvider for PartialStateReader<N> {
    fn storage(&self, address: Address, key: StorageKey) -> ProviderResult<Option<StorageValue>> {
        Self::storage(self, address, key)
    }

    fn account_code(&self, address: &Address) -> ProviderResult<Option<Bytecode>> {
        Self::account_code(self, address)
    }
}

impl<N: ProviderNodeTypes> BlockHashReader for PartialStateReader<N> {
    fn block_hash(&self, number: BlockNumber) -> ProviderResult<Option<B256>> {
        if number > self.checkpoint.pivot.block_number {
            return Ok(None)
        }
        self.block_hashes
            .get(&number)
            .copied()
            .map(Some)
            .ok_or_else(|| ProviderError::HeaderNotFound(number.into()))
    }

    fn canonical_hashes_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> ProviderResult<Vec<B256>> {
        (start..end)
            .map(|number| {
                self.block_hash(number).and_then(|hash| {
                    hash.ok_or_else(|| ProviderError::HeaderNotFound(number.into()))
                })
            })
            .collect()
    }
}

impl<N: ProviderNodeTypes> HashedPostStateProvider for PartialStateReader<N> {
    fn hashed_post_state(&self, state: &BundleState) -> HashedPostState {
        HashedPostState::from_bundle_state::<KeccakKeyHasher>(state.state())
    }
}

// Full-state trie providers cannot be used here: their nodes need not describe this checkpoint.
impl<N: ProviderNodeTypes> StateRootProvider for PartialStateReader<N> {
    fn state_root(&self, state: HashedPostState) -> ProviderResult<B256> {
        if state.is_empty() {
            return Ok(self.checkpoint.pivot.state_root)
        }
        Err(PartialStateReadError::Unsupported("state root with an execution overlay").into())
    }

    fn state_root_from_nodes(&self, _input: TrieInput) -> ProviderResult<B256> {
        Err(PartialStateReadError::Unsupported("state root from trie nodes").into())
    }

    fn state_root_with_updates(
        &self,
        _state: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        Err(PartialStateReadError::Unsupported("state root with trie updates").into())
    }

    fn state_root_from_nodes_with_updates(
        &self,
        _input: TrieInput,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        Err(PartialStateReadError::Unsupported("state root with trie updates").into())
    }
}

impl<N: ProviderNodeTypes> StorageRootProvider for PartialStateReader<N> {
    fn storage_root(&self, address: Address, storage: HashedStorage) -> ProviderResult<B256> {
        if storage.is_empty() {
            return Ok(self.account_storage_root(&address)?.unwrap_or(EMPTY_ROOT_HASH))
        }
        Err(PartialStateReadError::Unsupported("storage root with an execution overlay").into())
    }

    fn storage_proof(
        &self,
        _address: Address,
        _slot: B256,
        _storage: HashedStorage,
    ) -> ProviderResult<StorageProof> {
        Err(PartialStateReadError::Unsupported("storage proofs").into())
    }

    fn storage_multiproof(
        &self,
        _address: Address,
        _slots: &[B256],
        _storage: HashedStorage,
    ) -> ProviderResult<StorageMultiProof> {
        Err(PartialStateReadError::Unsupported("storage proofs").into())
    }
}

impl<N: ProviderNodeTypes> StateProofProvider for PartialStateReader<N> {
    fn proof(
        &self,
        _input: TrieInput,
        _address: Address,
        _slots: &[B256],
    ) -> ProviderResult<AccountProof> {
        Err(PartialStateReadError::Unsupported("account proofs").into())
    }

    fn multiproof(
        &self,
        _input: TrieInput,
        _targets: MultiProofTargets,
    ) -> ProviderResult<MultiProof> {
        Err(PartialStateReadError::Unsupported("account proofs").into())
    }

    fn witness(
        &self,
        _input: TrieInput,
        _target: HashedPostState,
        _mode: ExecutionWitnessMode,
    ) -> ProviderResult<Vec<Bytes>> {
        Err(PartialStateReadError::Unsupported("execution witnesses").into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{create_test_provider_factory, MockNodeTypesWithDB};
    use alloy_eip7928::{BalanceChange, CodeChange, SlotChanges, StorageChange};
    use alloy_eips::{eip7928::compute_block_access_list_hash, BlockNumHash};
    use alloy_primitives::{address, Bytes, U256};
    use reth_db_api::transaction::DbTxMut;
    use reth_primitives_traits::StorageEntry;
    use reth_storage_api::{
        AccountInfoReader, DBProvider, PartialStateCheckpointProvider,
        PartialStateCheckpointStatus, PartialStateResolvedAccounts, PartialStateSnapWriter,
        PartialStateTransition, PartialStateTransitionProvider,
    };
    use reth_trie::root::{state_root_unsorted, storage_root};
    use reth_trie_common::TrieAccount;

    const TRACKED: Address = address!("0000000000000000000000000000000000000001");
    const UNTRACKED: Address = address!("0000000000000000000000000000000000000002");
    const EMPTY: Address = address!("0000000000000000000000000000000000000003");
    const SHARED: Address = address!("0000000000000000000000000000000000000004");
    const ABSENT: Address = address!("0000000000000000000000000000000000000005");
    const SLOT: B256 = B256::repeat_byte(0x11);
    const CODE: &[u8] = &[0x60, 0x01];
    const UNTRACKED_CODE: &[u8] = &[0x60, 0x02];

    #[test]
    fn state_provider_exposes_commitments_but_rejects_unavailable_trie_data() {
        let (factory, filter, pivot) = setup();
        let reader = factory.partial_state_reader(pivot, &filter).unwrap();
        assert_eq!(reader.state_root(HashedPostState::default()).unwrap(), pivot.state_root);
        assert_eq!(
            reader.storage_root(UNTRACKED, HashedStorage::default()).unwrap(),
            reader.account_storage_root(&UNTRACKED).unwrap().unwrap()
        );
        assert!(reader.storage_root(TRACKED, HashedStorage::new(true)).is_err());
        assert!(reader.state_root_with_updates(HashedPostState::default()).is_err());
        assert!(reader.proof(TrieInput::default(), TRACKED, &[]).is_err());
        assert_eq!(reader.block_hash(pivot.block_number).unwrap(), Some(pivot.block_hash));
        assert!(reader.block_hash(pivot.block_number - 1).is_err());
        assert_eq!(reader.block_hash(pivot.block_number + 1).unwrap(), None);
    }

    fn accounts() -> [(Address, TrieAccount); 4] {
        [
            (
                TRACKED,
                TrieAccount {
                    nonce: 7,
                    balance: U256::from(100),
                    storage_root: storage_root([(keccak256(SLOT), U256::from(42))]),
                    code_hash: keccak256(CODE),
                },
            ),
            (
                UNTRACKED,
                TrieAccount {
                    nonce: 8,
                    balance: U256::from(200),
                    storage_root: storage_root([(keccak256(SLOT), U256::from(9))]),
                    code_hash: keccak256(UNTRACKED_CODE),
                },
            ),
            (
                EMPTY,
                TrieAccount {
                    nonce: 1,
                    balance: U256::from(300),
                    storage_root: EMPTY_ROOT_HASH,
                    code_hash: KECCAK_EMPTY,
                },
            ),
            (
                SHARED,
                TrieAccount {
                    nonce: 1,
                    balance: U256::from(400),
                    storage_root: EMPTY_ROOT_HASH,
                    code_hash: keccak256(CODE),
                },
            ),
        ]
    }

    fn setup(
    ) -> (ProviderFactory<MockNodeTypesWithDB>, ConfiguredContractFilter, PartialStateSnapPivot)
    {
        let factory = create_test_provider_factory();
        let filter = ConfiguredContractFilter::new([TRACKED, ABSENT]);
        let accounts = accounts();
        let pivot = PartialStateSnapPivot {
            block_number: 7,
            block_hash: B256::repeat_byte(0x77),
            state_root: state_root_unsorted(
                accounts.iter().map(|(address, account)| (keccak256(address), *account)),
            ),
        };
        factory.begin_partial_state_sync(pivot, &filter).unwrap();
        let provider = factory.database_provider_rw().unwrap();
        let mut writer = provider.partial_state_snap_writer();
        for (address, account) in accounts {
            writer.write_account(keccak256(address), account).unwrap();
        }
        writer.write_storage(keccak256(TRACKED), keccak256(SLOT), U256::from(42)).unwrap();
        writer.write_bytecode(keccak256(CODE), CODE).unwrap();
        provider.commit().unwrap();
        factory.complete_partial_state_sync(pivot, &filter).unwrap();
        (factory, filter, pivot)
    }

    #[test]
    fn reads_verified_partial_tables_without_full_state() {
        let (factory, filter, pivot) = setup();
        let provider = factory.database_provider_ro().unwrap();
        assert_eq!(provider.tx_ref().entries::<tables::PlainAccountState>().unwrap(), 0);
        assert_eq!(provider.tx_ref().entries::<tables::HashedAccounts>().unwrap(), 0);
        assert_eq!(provider.tx_ref().entries::<tables::PlainStorageState>().unwrap(), 0);
        assert_eq!(provider.tx_ref().entries::<tables::HashedStorages>().unwrap(), 0);

        let reader = factory.partial_state_reader(pivot, &filter).unwrap();
        fn assert_account_info_reader(_: &impl AccountInfoReader) {}
        assert_account_info_reader(&reader);
        assert_eq!(reader.checkpoint().pivot, pivot);
        assert_eq!(reader.checkpoint().filter_hash, filter.filter_hash());
        assert_eq!(reader.checkpoint().status, PartialStateCheckpointStatus::Complete);
        for (address, account) in accounts() {
            assert_eq!(reader.basic_account(&address).unwrap(), Some(Account::from(account)));
            assert_eq!(reader.account_storage_root(&address).unwrap(), Some(account.storage_root));
        }
        assert_eq!(reader.storage(TRACKED, SLOT).unwrap(), Some(U256::from(42)));
        assert_eq!(reader.account_code(&TRACKED).unwrap().unwrap().original_bytes().as_ref(), CODE);
        assert_eq!(
            reader.bytecode_by_hash(&keccak256(CODE)).unwrap(),
            reader.account_code(&TRACKED).unwrap()
        );
    }

    #[test]
    fn distinguishes_known_empty_state_from_untracked_state() {
        let (factory, filter, pivot) = setup();
        let reader = factory.partial_state_reader(pivot, &filter).unwrap();
        assert_eq!(reader.basic_account(&ABSENT).unwrap(), None);
        assert_eq!(reader.account_storage_root(&ABSENT).unwrap(), None);
        assert_eq!(reader.storage(ABSENT, SLOT).unwrap(), None);
        assert_eq!(reader.account_code(&ABSENT).unwrap(), None);
        assert_eq!(reader.storage(EMPTY, SLOT).unwrap(), None);
        assert_eq!(reader.account_code(&EMPTY).unwrap(), None);
        assert_eq!(reader.bytecode_by_hash(&KECCAK_EMPTY).unwrap(), None);
        assert_eq!(reader.storage(SHARED, SLOT).unwrap(), None);
        // Seeking a missing key may return the next larger slot, which must not be treated as a
        // hit.
        let absent_slot = (0..=u8::MAX)
            .map(B256::repeat_byte)
            .find(|slot| keccak256(slot) < keccak256(SLOT))
            .unwrap();
        assert_eq!(reader.storage(TRACKED, absent_slot).unwrap(), None);
        assert!(matches!(
            reader.storage(UNTRACKED, SLOT),
            Err(ProviderError::StorageNotTracked(address)) if address == UNTRACKED
        ));
        assert!(matches!(
            reader.account_code(&UNTRACKED),
            Err(ProviderError::CodeNotTracked(address)) if address == UNTRACKED
        ));
        assert!(matches!(
            reader.account_code(&SHARED),
            Err(ProviderError::CodeNotTracked(address)) if address == SHARED
        ));
    }

    #[test]
    fn does_not_fall_back_to_full_state_or_untracked_code() {
        let (factory, filter, pivot) = setup();
        let provider = factory.database_provider_rw().unwrap();
        let bogus = Account { nonce: 99, balance: U256::from(999), bytecode_hash: None };
        for address in [TRACKED, ABSENT] {
            provider.tx_ref().put::<tables::PlainAccountState>(address, bogus).unwrap();
            provider.tx_ref().put::<tables::HashedAccounts>(keccak256(address), bogus).unwrap();
            provider
                .tx_ref()
                .put::<tables::PlainStorageState>(address, StorageEntry::new(SLOT, U256::from(999)))
                .unwrap();
            provider
                .tx_ref()
                .put::<tables::HashedStorages>(
                    keccak256(address),
                    StorageEntry::new(keccak256(SLOT), U256::from(999)),
                )
                .unwrap();
        }
        provider
            .partial_state_snap_writer()
            .write_bytecode(keccak256(UNTRACKED_CODE), UNTRACKED_CODE)
            .unwrap();
        // Even an accidentally retained untracked slot must not bypass the filter.
        provider
            .partial_state_snap_writer()
            .write_storage(keccak256(UNTRACKED), keccak256(SLOT), U256::from(9))
            .unwrap();
        provider.commit().unwrap();

        let reader = factory.partial_state_reader(pivot, &filter).unwrap();
        assert_eq!(reader.basic_account(&TRACKED).unwrap().unwrap().nonce, 7);
        assert_eq!(reader.basic_account(&ABSENT).unwrap(), None);
        assert_eq!(reader.storage(TRACKED, SLOT).unwrap(), Some(U256::from(42)));
        assert_eq!(reader.storage(ABSENT, SLOT).unwrap(), None);
        assert!(matches!(
            reader.storage(UNTRACKED, SLOT),
            Err(ProviderError::StorageNotTracked(_))
        ));
        assert!(matches!(
            reader.bytecode_by_hash(&keccak256(UNTRACKED_CODE)),
            Err(ProviderError::PartialStateRead(error))
                if *error == PartialStateReadError::CodeHashNotTracked(keccak256(UNTRACKED_CODE))
        ));
    }

    #[test]
    fn rejects_missing_and_incomplete_checkpoints() {
        let factory = create_test_provider_factory();
        let filter = ConfiguredContractFilter::default();
        let pivot = PartialStateSnapPivot {
            block_number: 0,
            block_hash: B256::repeat_byte(1),
            state_root: EMPTY_ROOT_HASH,
        };
        assert!(matches!(
            factory.partial_state_reader(pivot, &filter),
            Err(ProviderError::PartialStateCheckpoint(error))
                if *error == PartialStateCheckpointError::Unavailable
        ));
        factory.begin_partial_state_sync(pivot, &filter).unwrap();
        assert!(matches!(
            factory.partial_state_reader(pivot, &filter),
            Err(ProviderError::PartialStateCheckpoint(error))
                if matches!(*error, PartialStateCheckpointError::Incomplete { .. })
        ));
        factory.complete_partial_state_sync(pivot, &filter).unwrap();
        assert!(factory.partial_state_reader(pivot, &filter).is_ok());
    }

    #[test]
    fn rejects_mismatched_filter_block_hash_number_and_root() {
        let (factory, filter, pivot) = setup();
        let changed = ConfiguredContractFilter::new([UNTRACKED]);
        assert!(matches!(
            factory.partial_state_reader(pivot, &changed),
            Err(ProviderError::PartialStateCheckpoint(error))
                if matches!(*error, PartialStateCheckpointError::FilterMismatch { .. })
        ));
        for requested in [
            PartialStateSnapPivot { block_number: pivot.block_number + 1, ..pivot },
            PartialStateSnapPivot { block_hash: B256::repeat_byte(0xff), ..pivot },
            PartialStateSnapPivot { state_root: EMPTY_ROOT_HASH, ..pivot },
        ] {
            assert!(matches!(
                factory.partial_state_reader(requested, &filter),
                Err(ProviderError::PartialStateCheckpoint(error))
                    if matches!(*error, PartialStateCheckpointError::PivotMismatch { .. })
            ));
        }
    }

    #[test]
    fn reports_missing_and_corrupt_tracked_bytecode() {
        let (factory, filter, pivot) = setup();
        let code_hash = keccak256(CODE);
        let provider = factory.database_provider_rw().unwrap();
        provider.tx_ref().delete::<tables::Bytecodes>(code_hash, None).unwrap();
        provider.commit().unwrap();
        let reader = factory.partial_state_reader(pivot, &filter).unwrap();
        assert!(matches!(
            reader.account_code(&TRACKED),
            Err(ProviderError::PartialStateRead(error))
                if *error == PartialStateReadError::MissingBytecode(code_hash)
        ));
        assert!(matches!(
            reader.bytecode_by_hash(&code_hash),
            Err(ProviderError::PartialStateRead(error))
                if *error == PartialStateReadError::MissingBytecode(code_hash)
        ));

        let provider = factory.database_provider_rw().unwrap();
        provider.partial_state_snap_writer().write_bytecode(code_hash, UNTRACKED_CODE).unwrap();
        provider.commit().unwrap();
        let reader = factory.partial_state_reader(pivot, &filter).unwrap();
        assert!(matches!(
            reader.account_code(&TRACKED),
            Err(ProviderError::PartialStateRead(error))
                if *error == PartialStateReadError::BytecodeHashMismatch {
                    expected: code_hash, computed: keccak256(UNTRACKED_CODE)
                }
        ));
    }

    #[test]
    fn holds_snapshot_across_sync_reset() {
        let (factory, filter, pivot) = setup();
        let reader = factory.partial_state_reader(pivot, &filter).unwrap();
        let next = PartialStateSnapPivot {
            block_number: 10,
            block_hash: B256::repeat_byte(0xaa),
            state_root: EMPTY_ROOT_HASH,
        };
        factory.begin_partial_state_sync(next, &filter).unwrap();
        assert!(matches!(
            factory.partial_state_reader(next, &filter),
            Err(ProviderError::PartialStateCheckpoint(error))
                if matches!(*error, PartialStateCheckpointError::Incomplete { .. })
        ));
        assert_eq!(reader.checkpoint().pivot, pivot);
        assert_eq!(reader.storage(TRACKED, SLOT).unwrap(), Some(U256::from(42)));
        factory.complete_partial_state_sync(next, &filter).unwrap();
        let new_reader = factory.partial_state_reader(next, &filter).unwrap();
        assert_eq!(new_reader.basic_account(&TRACKED).unwrap(), None);
        assert_eq!(new_reader.storage(TRACKED, SLOT).unwrap(), None);
        assert!(matches!(
            new_reader.bytecode_by_hash(&keccak256(CODE)),
            Err(ProviderError::PartialStateRead(error))
                if matches!(*error, PartialStateReadError::CodeHashNotTracked(_))
        ));
        assert_eq!(reader.basic_account(&TRACKED).unwrap().unwrap().nonce, 7);
        assert_eq!(reader.account_code(&TRACKED).unwrap().unwrap().original_bytes().as_ref(), CODE);
    }

    #[test]
    fn holds_snapshot_across_bal_transition_and_revert() {
        let (factory, filter, pivot) = setup();
        let parent_reader = factory.partial_state_reader(pivot, &filter).unwrap();
        let child_code = Bytes::from_static(&[0x60, 0x03]);
        let child_code_hash = keccak256(&child_code);
        let mut child_accounts = accounts();
        child_accounts[0].1.balance = U256::from(101);
        child_accounts[0].1.storage_root = storage_root([(keccak256(SLOT), U256::from(43))]);
        child_accounts[0].1.code_hash = child_code_hash;
        let child = PartialStateSnapPivot {
            block_number: pivot.block_number + 1,
            block_hash: B256::repeat_byte(0x88),
            state_root: state_root_unsorted(
                child_accounts.iter().map(|(address, account)| (keccak256(address), *account)),
            ),
        };
        let access_list = vec![alloy_eip7928::AccountChanges::new(TRACKED)
            .with_balance_change(BalanceChange::new(1, U256::from(101)))
            .with_storage_change(SlotChanges::new(
                U256::from_be_bytes(SLOT.0),
                vec![StorageChange::new(1, U256::from(43))],
            ))
            .with_code_change(CodeChange::new(1, child_code))];
        factory
            .apply_partial_state_transition(
                PartialStateTransition {
                    block: BlockNumHash::new(child.block_number, child.block_hash),
                    parent_block_hash: pivot.block_hash,
                    parent_root: pivot.state_root,
                    expected_root: child.state_root,
                    expected_bal_hash: compute_block_access_list_hash(&access_list),
                    access_list: &access_list,
                    resolved_accounts: &PartialStateResolvedAccounts::new(),
                },
                &filter,
            )
            .unwrap();
        let child_reader = factory.partial_state_reader(child, &filter).unwrap();
        assert_eq!(child_reader.basic_account(&TRACKED).unwrap().unwrap().balance, U256::from(101));
        assert_eq!(child_reader.storage(TRACKED, SLOT).unwrap(), Some(U256::from(43)));
        assert_eq!(
            child_reader.account_code(&TRACKED).unwrap().unwrap().original_bytes().as_ref(),
            &[0x60, 0x03]
        );
        assert_eq!(parent_reader.checkpoint().pivot, pivot);
        assert_eq!(
            parent_reader.basic_account(&TRACKED).unwrap().unwrap().balance,
            U256::from(100)
        );
        assert_eq!(parent_reader.storage(TRACKED, SLOT).unwrap(), Some(U256::from(42)));
        assert_eq!(
            parent_reader.account_code(&TRACKED).unwrap().unwrap().original_bytes().as_ref(),
            CODE
        );
        assert!(factory.partial_state_reader(pivot, &filter).is_err());
        assert!(matches!(
            child_reader.bytecode_by_hash(&keccak256(CODE)),
            Err(ProviderError::PartialStateRead(error))
                if matches!(*error, PartialStateReadError::CodeHashNotTracked(_))
        ));

        let restored = factory
            .revert_partial_state_transition(
                BlockNumHash::new(child.block_number, child.block_hash),
                &filter,
            )
            .unwrap();
        assert_eq!(restored, pivot);
        let restored_reader = factory.partial_state_reader(pivot, &filter).unwrap();
        assert_eq!(restored_reader.storage(TRACKED, SLOT).unwrap(), Some(U256::from(42)));
        assert_eq!(
            restored_reader.account_code(&TRACKED).unwrap().unwrap().original_bytes().as_ref(),
            CODE
        );
        // Reverted bytecode can remain in the shared table but is no longer readable at the parent.
        assert!(matches!(
            restored_reader.bytecode_by_hash(&child_code_hash),
            Err(ProviderError::PartialStateRead(error))
                if matches!(*error, PartialStateReadError::CodeHashNotTracked(_))
        ));
        assert_eq!(child_reader.checkpoint().pivot, child);
        assert_eq!(child_reader.storage(TRACKED, SLOT).unwrap(), Some(U256::from(43)));
    }
}
