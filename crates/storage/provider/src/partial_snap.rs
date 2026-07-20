use crate::{providers::NodeTypesForProvider, DatabaseProvider};
use alloy_primitives::{Bytes, B256, U256};
use reth_db_api::{
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::{Account, Bytecode, StorageEntry};
use reth_storage_api::PartialStateSnapWriter;
use reth_storage_errors::provider::ProviderError;
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
        self.provider
            .tx_ref()
            .put::<tables::HashedAccounts>(account_hash, Account::from(account))
            .map_err(Into::into)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_test_provider_factory;
    use alloy_consensus::constants::KECCAK_EMPTY;
    use reth_db_api::{cursor::DbDupCursorRO, transaction::DbTx};

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
                    storage_root: B256::ZERO,
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
    }
}
