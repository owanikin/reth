use crate::{
    providers::{NodeTypesForProvider, ProviderNodeTypes},
    BlockNumReader, DatabaseProvider, HeaderProvider, ProviderFactory,
};
use alloy_consensus::{constants::EMPTY_ROOT_HASH, BlockHeader};
use alloy_primitives::{Bytes, B256, U256};
use reth_db_api::{
    cursor::{DbCursorRO, DbDupCursorRO},
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::{Account, Bytecode, StorageEntry};
use reth_storage_api::{
    ContractFilter, DatabaseProviderFactory, PartialStateRootProvider, PartialStateSnapAccount,
    PartialStateSnapAccountRange, PartialStateSnapByteCodes, PartialStateSnapPivot,
    PartialStateSnapProvider, PartialStateSnapStorage, PartialStateSnapStorageRanges,
    PartialStateSnapTrieNodes, PartialStateSnapTriePath, PartialStateSnapWriter,
};
use reth_storage_errors::provider::{ProviderError, SnapStateRootUnavailableError};
use reth_trie::root::{StateRootBuilder, StorageRootBuilder};
use reth_trie_common::TrieAccount;

/// Writes partial snap state responses into Reth's hash-keyed state tables.
#[derive(Debug)]
pub struct PartialStateSnapDbWriter<'a, TX, N: NodeTypesForProvider> {
    provider: &'a DatabaseProvider<TX, N>,
}

impl<'a, TX, N> PartialStateSnapDbWriter<'a, TX, N>
where
    N: NodeTypesForProvider,
{
    /// Creates a new partial snap state writer backed by a database provider.
    pub const fn new(provider: &'a DatabaseProvider<TX, N>) -> Self {
        Self { provider }
    }
}

impl<TX, N> DatabaseProvider<TX, N>
where
    TX: DbTx + DbTxMut + 'static,
    N: NodeTypesForProvider,
{
    /// Returns a writer that persists partial snap state responses into hash-keyed state tables.
    pub const fn partial_state_snap_writer(&self) -> PartialStateSnapDbWriter<'_, TX, N> {
        PartialStateSnapDbWriter::new(self)
    }
}

impl<TX, N> PartialStateSnapWriter for PartialStateSnapDbWriter<'_, TX, N>
where
    TX: DbTx + DbTxMut + 'static,
    N: NodeTypesForProvider,
{
    type Error = ProviderError;

    fn write_account(
        &mut self,
        account_hash: B256,
        account: TrieAccount,
    ) -> Result<(), Self::Error> {
        let storage_root = account.storage_root;
        self.provider
            .tx_ref()
            .put::<tables::HashedAccounts>(account_hash, Account::from(account))?;

        if storage_root == EMPTY_ROOT_HASH {
            self.provider
                .tx_ref()
                .delete::<tables::PartialStateStorageRoots>(account_hash, None)?;
        } else {
            self.provider
                .tx_ref()
                .put::<tables::PartialStateStorageRoots>(account_hash, storage_root)?;
        }
        Ok(())
    }

    fn write_storage(
        &mut self,
        account_hash: B256,
        slot_hash: B256,
        value: U256,
    ) -> Result<(), Self::Error> {
        self.provider
            .tx_ref()
            .put::<tables::HashedStorages>(account_hash, StorageEntry::new(slot_hash, value))
            .map_err(Into::into)
    }

    fn write_bytecode(&mut self, code_hash: B256, bytecode: &[u8]) -> Result<(), Self::Error> {
        let bytecode = Bytecode::new_raw_checked(Bytes::copy_from_slice(bytecode))
            .map_err(ProviderError::other)?;
        self.provider.tx_ref().put::<tables::Bytecodes>(code_hash, bytecode).map_err(Into::into)
    }
}

impl<N> PartialStateSnapProvider for ProviderFactory<N>
where
    N: ProviderNodeTypes,
{
    fn snap_state_pivot(&self) -> Result<PartialStateSnapPivot, ProviderError> {
        self.database_provider_ro()?.snap_state_pivot()
    }

    fn snap_account_range(
        &self,
        root_hash: B256,
        starting_hash: B256,
        limit_hash: B256,
        response_bytes: u64,
    ) -> Result<PartialStateSnapAccountRange, ProviderError> {
        self.database_provider_ro()?.snap_account_range(
            root_hash,
            starting_hash,
            limit_hash,
            response_bytes,
        )
    }

    fn snap_storage_ranges(
        &self,
        root_hash: B256,
        account_hashes: &[B256],
        starting_hash: B256,
        limit_hash: B256,
        response_bytes: u64,
    ) -> Result<PartialStateSnapStorageRanges, ProviderError> {
        self.database_provider_ro()?.snap_storage_ranges(
            root_hash,
            account_hashes,
            starting_hash,
            limit_hash,
            response_bytes,
        )
    }

    fn snap_bytecodes(
        &self,
        hashes: &[B256],
        response_bytes: u64,
    ) -> Result<PartialStateSnapByteCodes, ProviderError> {
        self.database_provider_ro()?.snap_bytecodes(hashes, response_bytes)
    }

    fn snap_trie_nodes(
        &self,
        root_hash: B256,
        paths: &[PartialStateSnapTriePath],
        response_bytes: u64,
    ) -> Result<PartialStateSnapTrieNodes, ProviderError> {
        self.database_provider_ro()?.snap_trie_nodes(root_hash, paths, response_bytes)
    }
}

impl<N> PartialStateRootProvider for ProviderFactory<N>
where
    N: ProviderNodeTypes,
{
    fn partial_state_root(&self, filter: &dyn ContractFilter) -> Result<B256, ProviderError> {
        self.database_provider_ro()?.partial_state_root(filter)
    }
}

impl<TX, N> PartialStateSnapProvider for DatabaseProvider<TX, N>
where
    TX: DbTx + Send + Sync + 'static,
    N: NodeTypesForProvider,
{
    fn snap_state_pivot(&self) -> Result<PartialStateSnapPivot, ProviderError> {
        // Full block persistence advances Finish in the same transaction as hashed state, so this
        // header commits to the exact database state visible through this provider transaction.
        let block_number = self.best_block_number()?;
        let header = self
            .sealed_header(block_number)?
            .ok_or_else(|| ProviderError::HeaderNotFound(block_number.into()))?;

        Ok(PartialStateSnapPivot {
            block_number,
            block_hash: header.hash(),
            state_root: header.state_root(),
        })
    }

    fn snap_account_range(
        &self,
        root_hash: B256,
        starting_hash: B256,
        limit_hash: B256,
        response_bytes: u64,
    ) -> Result<PartialStateSnapAccountRange, ProviderError> {
        self.ensure_snap_state_root(root_hash)?;

        let mut cursor = self.tx_ref().cursor_read::<tables::HashedAccounts>()?;
        let mut next = cursor.seek(starting_hash)?;
        let mut accounts = Vec::new();
        let mut total_bytes = 0u64;

        while let Some((account_hash, account)) = next {
            if account_hash > limit_hash {
                break
            }

            let storage_root = self.account_storage_root(account_hash)?;
            let trie_account = account.into_trie_account(storage_root);
            if !accounts.is_empty() &&
                exceeds_soft_limit(total_bytes, ACCOUNT_RANGE_ITEM_BYTES, response_bytes)
            {
                break
            }

            accounts.push(PartialStateSnapAccount { hash: account_hash, account: trie_account });
            total_bytes = total_bytes.saturating_add(ACCOUNT_RANGE_ITEM_BYTES);
            if response_bytes != 0 && total_bytes >= response_bytes {
                break
            }
            next = cursor.next()?;
        }

        Ok(PartialStateSnapAccountRange { accounts, proof: Vec::new() })
    }

    fn snap_storage_ranges(
        &self,
        root_hash: B256,
        account_hashes: &[B256],
        starting_hash: B256,
        limit_hash: B256,
        response_bytes: u64,
    ) -> Result<PartialStateSnapStorageRanges, ProviderError> {
        self.ensure_snap_state_root(root_hash)?;

        let mut cursor = self.tx_ref().cursor_dup_read::<tables::HashedStorages>()?;
        let mut slots = Vec::with_capacity(account_hashes.len());
        let mut total_bytes = 0u64;

        for account_hash in account_hashes {
            if response_bytes != 0 && total_bytes >= response_bytes {
                break
            }

            let mut account_slots = Vec::new();
            let mut next = cursor.seek_by_key_subkey(*account_hash, starting_hash)?;

            while let Some(entry) = next {
                if entry.key > limit_hash {
                    break
                }

                if !account_slots.is_empty() &&
                    exceeds_soft_limit(total_bytes, STORAGE_RANGE_ITEM_BYTES, response_bytes)
                {
                    break
                }

                account_slots.push(PartialStateSnapStorage { hash: entry.key, value: entry.value });
                total_bytes = total_bytes.saturating_add(STORAGE_RANGE_ITEM_BYTES);
                if response_bytes != 0 && total_bytes >= response_bytes {
                    break
                }
                next = cursor.next_dup_val()?;
            }

            slots.push(account_slots);
        }

        Ok(PartialStateSnapStorageRanges { slots, proof: Vec::new() })
    }

    fn snap_bytecodes(
        &self,
        hashes: &[B256],
        response_bytes: u64,
    ) -> Result<PartialStateSnapByteCodes, ProviderError> {
        let mut codes = Vec::new();
        let mut total_bytes = 0u64;

        for hash in hashes {
            let Some(bytecode) = self.tx_ref().get::<tables::Bytecodes>(*hash)? else { continue };
            let code = bytecode.original_bytes();
            let code_len = code.len() as u64;
            if !codes.is_empty() && exceeds_soft_limit(total_bytes, code_len, response_bytes) {
                break
            }

            codes.push(code);
            total_bytes = total_bytes.saturating_add(code_len);
            if response_bytes != 0 && total_bytes >= response_bytes {
                break
            }
        }

        Ok(PartialStateSnapByteCodes { codes })
    }

    fn snap_trie_nodes(
        &self,
        root_hash: B256,
        _paths: &[PartialStateSnapTriePath],
        _response_bytes: u64,
    ) -> Result<PartialStateSnapTrieNodes, ProviderError> {
        self.ensure_snap_state_root(root_hash)?;
        Ok(PartialStateSnapTrieNodes::default())
    }
}

impl<TX, N> PartialStateRootProvider for DatabaseProvider<TX, N>
where
    TX: DbTx + Send + Sync + 'static,
    N: NodeTypesForProvider,
{
    fn partial_state_root(&self, filter: &dyn ContractFilter) -> Result<B256, ProviderError> {
        let mut account_cursor = self.tx_ref().cursor_read::<tables::HashedAccounts>()?;
        let mut commitment_cursor =
            self.tx_ref().cursor_read::<tables::PartialStateStorageRoots>()?;
        let mut storage_cursor = self.tx_ref().cursor_dup_read::<tables::HashedStorages>()?;
        let mut next_account = account_cursor.seek(B256::ZERO)?;
        let mut next_commitment = commitment_cursor.seek(B256::ZERO)?;
        let mut root_builder = StateRootBuilder::default();

        while let Some((account_hash, account)) = next_account {
            while next_commitment.is_some_and(|(hash, _)| hash < account_hash) {
                next_commitment = commitment_cursor.next()?;
            }
            let preserved_storage_root =
                next_commitment.filter(|(hash, _)| *hash == account_hash).map(|(_, root)| root);
            if preserved_storage_root.is_some() {
                next_commitment = commitment_cursor.next()?;
            }

            let storage_root = if filter.should_sync_storage_by_hash(&account_hash) {
                let mut storage_root_builder = StorageRootBuilder::default();
                let mut next_slot = storage_cursor.seek_by_key_subkey(account_hash, B256::ZERO)?;
                while let Some(entry) = next_slot {
                    storage_root_builder.add_storage(entry.key, entry.value);
                    next_slot = storage_cursor.next_dup_val()?;
                }
                storage_root_builder.root()
            } else {
                preserved_storage_root.unwrap_or(EMPTY_ROOT_HASH)
            };

            root_builder.add_account(account_hash, account.into_trie_account(storage_root));
            next_account = account_cursor.next()?;
        }

        Ok(root_builder.root())
    }
}

impl<TX, N> DatabaseProvider<TX, N>
where
    TX: DbTx + Send + Sync + 'static,
    N: NodeTypesForProvider,
{
    fn ensure_snap_state_root(&self, requested: B256) -> Result<(), ProviderError> {
        let available = self.snap_state_pivot()?.state_root;
        if requested != available {
            return Err(ProviderError::SnapStateRootUnavailable(Box::new(
                SnapStateRootUnavailableError { requested, available },
            )))
        }
        Ok(())
    }

    fn account_storage_root(&self, account_hash: B256) -> Result<B256, ProviderError> {
        if let Some(storage_root) =
            self.tx_ref().get::<tables::PartialStateStorageRoots>(account_hash)?
        {
            return Ok(storage_root)
        }
        self.storage_root_by_hash(account_hash)
    }

    fn storage_root_by_hash(&self, account_hash: B256) -> Result<B256, ProviderError> {
        let mut cursor = self.tx_ref().cursor_dup_read::<tables::HashedStorages>()?;
        let mut next = cursor.seek_by_key_subkey(account_hash, B256::ZERO)?;
        let mut root_builder = StorageRootBuilder::default();

        while let Some(entry) = next {
            root_builder.add_storage(entry.key, entry.value);
            next = cursor.next_dup_val()?;
        }

        Ok(root_builder.root())
    }
}

const ACCOUNT_RANGE_ITEM_BYTES: u64 = 32 + 8 + 32 + 32 + 32;
const STORAGE_RANGE_ITEM_BYTES: u64 = 32 + 32;

const fn exceeds_soft_limit(current: u64, next: u64, limit: u64) -> bool {
    limit != 0 && current.saturating_add(next) > limit
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_utils::create_test_provider_factory, StaticFileProviderFactory, StaticFileWriter,
    };
    use alloy_consensus::{constants::KECCAK_EMPTY, Header};
    use alloy_primitives::{address, keccak256};
    use reth_db_api::{cursor::DbDupCursorRO, transaction::DbTx};
    use reth_stages_types::{StageCheckpoint, StageId};
    use reth_static_file_types::StaticFileSegment;
    use reth_storage_api::{ConfiguredContractFilter, DBProvider, StageCheckpointWriter};
    use reth_trie::root::{state_root_unsorted, storage_root};

    fn install_persisted_snap_pivot(
        factory: &ProviderFactory<crate::test_utils::MockNodeTypesWithDB>,
        state_root: B256,
    ) -> PartialStateSnapPivot {
        let block_number = 0;
        let block_hash = B256::repeat_byte(0x42);
        let header = Header { number: block_number, state_root, ..Default::default() };

        {
            let static_file_provider = factory.static_file_provider();
            let mut writer =
                static_file_provider.latest_writer(StaticFileSegment::Headers).unwrap();
            writer.append_header(&header, &block_hash).unwrap();
            writer.commit().unwrap();
        }

        let provider = factory.database_provider_rw().unwrap();
        provider
            .save_stage_checkpoint(StageId::Finish, StageCheckpoint::new(block_number))
            .unwrap();
        provider.commit().unwrap();

        PartialStateSnapPivot { block_number, block_hash, state_root }
    }

    #[test]
    fn partial_snap_writer_persists_hash_keyed_state() {
        let factory = create_test_provider_factory();
        let provider = factory.database_provider_rw().unwrap();
        let account_hash = B256::repeat_byte(0x11);
        let storage_root = B256::repeat_byte(0x22);
        let code_hash = B256::repeat_byte(0x33);
        let slot_hash = B256::repeat_byte(0x44);
        let storage_value = U256::from(42);
        let bytecode = [0x60, 0x00];

        {
            let mut writer = provider.partial_state_snap_writer();
            writer
                .write_account(
                    account_hash,
                    TrieAccount { nonce: 7, balance: U256::from(100), storage_root, code_hash },
                )
                .unwrap();
            writer.write_storage(account_hash, slot_hash, storage_value).unwrap();
            writer.write_bytecode(code_hash, &bytecode).unwrap();
        }

        let stored_account = provider
            .tx_ref()
            .get::<tables::HashedAccounts>(account_hash)
            .unwrap()
            .expect("account should be stored");
        assert_eq!(
            stored_account,
            Account { nonce: 7, balance: U256::from(100), bytecode_hash: Some(code_hash) }
        );
        assert_eq!(
            provider.tx_ref().get::<tables::PartialStateStorageRoots>(account_hash).unwrap(),
            Some(storage_root)
        );

        let mut storage_cursor =
            provider.tx_ref().cursor_dup_read::<tables::HashedStorages>().unwrap();
        let stored_storage = storage_cursor
            .seek_by_key_subkey(account_hash, slot_hash)
            .unwrap()
            .expect("storage should be stored");
        assert_eq!(stored_storage, StorageEntry::new(slot_hash, storage_value));

        let stored_bytecode = provider
            .tx_ref()
            .get::<tables::Bytecodes>(code_hash)
            .unwrap()
            .expect("bytecode should be stored");
        assert_eq!(
            stored_bytecode,
            Bytecode::new_raw_checked(Bytes::copy_from_slice(&bytecode)).unwrap()
        );
    }

    #[test]
    fn partial_snap_writer_stores_empty_code_hash_as_no_bytecode() {
        let factory = create_test_provider_factory();
        let provider = factory.database_provider_rw().unwrap();
        let account_hash = B256::repeat_byte(0x55);

        provider
            .partial_state_snap_writer()
            .write_account(
                account_hash,
                TrieAccount {
                    nonce: 0,
                    balance: U256::ZERO,
                    storage_root: EMPTY_ROOT_HASH,
                    code_hash: KECCAK_EMPTY,
                },
            )
            .unwrap();

        let stored_account = provider
            .tx_ref()
            .get::<tables::HashedAccounts>(account_hash)
            .unwrap()
            .expect("account should be stored");
        assert_eq!(stored_account.bytecode_hash, None);
        assert_eq!(
            provider.tx_ref().get::<tables::PartialStateStorageRoots>(account_hash).unwrap(),
            None
        );
    }

    #[test]
    fn partial_snap_provider_preserves_storage_root_without_local_slots() {
        let factory = create_test_provider_factory();
        let state_root = B256::repeat_byte(0x88);
        install_persisted_snap_pivot(&factory, state_root);
        let provider = factory.database_provider_rw().unwrap();
        let account_hash = B256::repeat_byte(0x66);
        let storage_root = B256::repeat_byte(0x77);
        let account = TrieAccount {
            nonce: 1,
            balance: U256::from(10),
            storage_root,
            code_hash: KECCAK_EMPTY,
        };

        provider.partial_state_snap_writer().write_account(account_hash, account).unwrap();

        assert_eq!(provider.storage_root_by_hash(account_hash).unwrap(), EMPTY_ROOT_HASH);

        let range =
            provider.snap_account_range(state_root, account_hash, account_hash, u64::MAX).unwrap();
        assert_eq!(range.accounts.len(), 1);
        assert_eq!(range.accounts[0].hash, account_hash);
        assert_eq!(range.accounts[0].account, account);
    }

    #[test]
    fn partial_snap_provider_reports_persisted_pivot() {
        let factory = create_test_provider_factory();
        let expected = install_persisted_snap_pivot(&factory, B256::repeat_byte(0x11));

        assert_eq!(factory.snap_state_pivot().unwrap(), expected);
    }

    #[test]
    fn partial_snap_provider_rejects_unavailable_state_root() {
        let factory = create_test_provider_factory();
        let available = B256::repeat_byte(0x11);
        let requested = B256::repeat_byte(0x22);
        install_persisted_snap_pivot(&factory, available);
        let provider = factory.database_provider_ro().unwrap();

        let account_error = provider
            .snap_account_range(requested, B256::ZERO, B256::repeat_byte(0xff), u64::MAX)
            .unwrap_err();
        assert!(matches!(
            account_error,
            ProviderError::SnapStateRootUnavailable(error)
                if error.requested == requested && error.available == available
        ));

        let storage_error = provider
            .snap_storage_ranges(
                requested,
                &[B256::ZERO],
                B256::ZERO,
                B256::repeat_byte(0xff),
                u64::MAX,
            )
            .unwrap_err();
        assert!(matches!(
            storage_error,
            ProviderError::SnapStateRootUnavailable(error)
                if error.requested == requested && error.available == available
        ));

        let trie_error = provider.snap_trie_nodes(requested, &[], u64::MAX).unwrap_err();
        assert!(matches!(
            trie_error,
            ProviderError::SnapStateRootUnavailable(error)
                if error.requested == requested && error.available == available
        ));
    }

    #[test]
    fn partial_snap_writer_removes_stale_empty_storage_commitment() {
        let factory = create_test_provider_factory();
        let provider = factory.database_provider_rw().unwrap();
        let account_hash = B256::repeat_byte(0x99);

        provider
            .partial_state_snap_writer()
            .write_account(
                account_hash,
                TrieAccount {
                    storage_root: B256::repeat_byte(0xaa),
                    code_hash: KECCAK_EMPTY,
                    ..Default::default()
                },
            )
            .unwrap();
        provider
            .partial_state_snap_writer()
            .write_account(
                account_hash,
                TrieAccount {
                    storage_root: EMPTY_ROOT_HASH,
                    code_hash: KECCAK_EMPTY,
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(
            provider.tx_ref().get::<tables::PartialStateStorageRoots>(account_hash).unwrap(),
            None
        );
    }

    #[test]
    fn partial_state_root_matches_complete_reference_state() {
        let factory = create_test_provider_factory();
        let provider = factory.database_provider_rw().unwrap();
        let tracked_address = address!("0000000000000000000000000000000000000001");
        let untracked_address = address!("0000000000000000000000000000000000000002");
        let empty_storage_address = address!("0000000000000000000000000000000000000003");
        let tracked_hash = keccak256(tracked_address);
        let untracked_hash = keccak256(untracked_address);
        let empty_storage_hash = keccak256(empty_storage_address);
        let tracked_slot = B256::repeat_byte(0x11);
        let untracked_slot = B256::repeat_byte(0x22);
        let tracked_value = U256::from(100);
        let untracked_value = U256::from(200);
        let tracked_storage_root = storage_root([(tracked_slot, tracked_value)]);
        let untracked_storage_root = storage_root([(untracked_slot, untracked_value)]);
        let tracked_account = TrieAccount {
            nonce: 1,
            balance: U256::from(10),
            storage_root: tracked_storage_root,
            code_hash: KECCAK_EMPTY,
        };
        let untracked_account = TrieAccount {
            nonce: 2,
            balance: U256::from(20),
            storage_root: untracked_storage_root,
            code_hash: KECCAK_EMPTY,
        };
        let empty_storage_account = TrieAccount {
            nonce: 3,
            balance: U256::from(30),
            storage_root: EMPTY_ROOT_HASH,
            code_hash: KECCAK_EMPTY,
        };
        let expected = state_root_unsorted([
            (tracked_hash, tracked_account),
            (untracked_hash, untracked_account),
            (empty_storage_hash, empty_storage_account),
        ]);

        {
            let mut writer = provider.partial_state_snap_writer();
            writer.write_account(tracked_hash, tracked_account).unwrap();
            writer.write_storage(tracked_hash, tracked_slot, tracked_value).unwrap();
            writer.write_account(untracked_hash, untracked_account).unwrap();
            writer.write_account(empty_storage_hash, empty_storage_account).unwrap();
        }

        // Tracked storage must be derived locally, even if its downloaded commitment is stale.
        provider
            .tx_ref()
            .put::<tables::PartialStateStorageRoots>(tracked_hash, B256::repeat_byte(0xff))
            .unwrap();

        let filter = ConfiguredContractFilter::new([tracked_address]);
        assert_eq!(provider.partial_state_root(&filter).unwrap(), expected);

        // Tracked storage is never substituted with its preserved commitment.
        provider
            .tx_ref()
            .delete::<tables::HashedStorages>(
                tracked_hash,
                Some(StorageEntry { key: tracked_slot, value: tracked_value }),
            )
            .unwrap();
        assert_ne!(provider.partial_state_root(&filter).unwrap(), expected);
        provider
            .partial_state_snap_writer()
            .write_storage(tracked_hash, tracked_slot, tracked_value)
            .unwrap();

        // Removing the only representation of untracked storage changes the resulting root,
        // allowing the caller to reject the partial state against the expected header root.
        provider.tx_ref().delete::<tables::PartialStateStorageRoots>(untracked_hash, None).unwrap();
        assert_ne!(provider.partial_state_root(&filter).unwrap(), expected);
    }
}
