//! Common root computation functions.

// Re-export for convenience.
#[doc(inline)]
pub use alloy_trie::root::{
    state_root, state_root_ref_unhashed, state_root_unhashed, state_root_unsorted, storage_root,
    storage_root_unhashed, storage_root_unsorted,
};

use crate::Nibbles;
use alloc::vec::Vec;
use alloy_primitives::{B256, U256};
use alloy_rlp::Encodable;
use alloy_trie::{HashBuilder, TrieAccount};

/// Streaming builder for an account trie root.
///
/// Unlike [`state_root`], this builder does not require collecting every account before root
/// calculation. Accounts must be added in ascending hashed-key order.
#[derive(Debug, Default)]
pub struct StateRootBuilder {
    hash_builder: HashBuilder,
    account_rlp: Vec<u8>,
}

impl StateRootBuilder {
    /// Adds an account leaf to the state trie.
    ///
    /// # Panics
    ///
    /// Panics if `hashed_key` is not greater than the previously added key.
    pub fn add_account(&mut self, hashed_key: B256, account: impl Into<TrieAccount>) {
        self.account_rlp.clear();
        account.into().encode(&mut self.account_rlp);
        self.hash_builder.add_leaf(Nibbles::unpack(hashed_key), &self.account_rlp);
    }

    /// Finishes the account trie and returns its root.
    pub fn root(mut self) -> B256 {
        self.hash_builder.root()
    }
}

/// Streaming builder for a storage trie root.
///
/// Storage slots must be added in ascending hashed-key order.
#[derive(Debug, Default)]
pub struct StorageRootBuilder {
    hash_builder: HashBuilder,
}

impl StorageRootBuilder {
    /// Adds a storage leaf to the storage trie.
    ///
    /// # Panics
    ///
    /// Panics if `hashed_key` is not greater than the previously added key.
    pub fn add_storage(&mut self, hashed_key: B256, value: U256) {
        self.hash_builder
            .add_leaf(Nibbles::unpack(hashed_key), alloy_rlp::encode_fixed_size(&value).as_ref());
    }

    /// Finishes the storage trie and returns its root.
    pub fn root(mut self) -> B256 {
        self.hash_builder.root()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streaming_state_root_matches_collected_state_root() {
        let accounts = [
            (
                B256::repeat_byte(0x11),
                TrieAccount {
                    nonce: 1,
                    balance: U256::from(10),
                    storage_root: B256::repeat_byte(0x22),
                    code_hash: B256::repeat_byte(0x33),
                },
            ),
            (
                B256::repeat_byte(0x44),
                TrieAccount {
                    nonce: 2,
                    balance: U256::from(20),
                    storage_root: B256::repeat_byte(0x55),
                    code_hash: B256::repeat_byte(0x66),
                },
            ),
        ];
        let expected = state_root(accounts.clone());
        let mut builder = StateRootBuilder::default();
        for (hashed_key, account) in accounts {
            builder.add_account(hashed_key, account);
        }

        assert_eq!(builder.root(), expected);
    }

    #[test]
    fn streaming_storage_root_matches_collected_storage_root() {
        let storage =
            [(B256::repeat_byte(0x11), U256::from(10)), (B256::repeat_byte(0x22), U256::from(20))];
        let expected = storage_root(storage);
        let mut builder = StorageRootBuilder::default();
        for (hashed_key, value) in storage {
            builder.add_storage(hashed_key, value);
        }

        assert_eq!(builder.root(), expected);
    }
}
