//! Helper types to workaround 'higher-ranked lifetime error'
//! <https://github.com/rust-lang/rust/issues/100013> in default implementation of
//! `reth_rpc_eth_api::helpers::Call`.

use alloy_consensus::constants::KECCAK_EMPTY;
use alloy_primitives::{Address, B256, U256};
use reth_errors::{ProviderError, ProviderResult};
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::{BytecodeReader, HashedPostStateProvider, StateProvider, StateProviderBox};
use reth_trie::{HashedStorage, MultiProofTargets};
use revm::database::{BundleState, State};

/// Helper alias type for the state's [`State`]
pub type StateCacheDb = State<StateProviderDatabase<StateProviderTraitObjWrapper>>;

/// Hack to get around 'higher-ranked lifetime error', see
/// <https://github.com/rust-lang/rust/issues/100013>
///
/// Apparently, when dealing with our RPC code, compiler is struggling to prove lifetimes around
/// [`StateProvider`] trait objects. This type is a workaround which should help the compiler to
/// understand that there are no lifetimes involved.
#[expect(missing_debug_implementations)]
pub struct StateProviderTraitObjWrapper {
    inner: StateProviderBox,
    partial_state_tracker: Option<std::sync::Arc<dyn Fn(&Address) -> bool + Send + Sync + 'static>>,
}

impl StateProviderTraitObjWrapper {
    /// Creates a new wrapper around a state provider.
    pub const fn new(inner: StateProviderBox) -> Self {
        Self { inner, partial_state_tracker: None }
    }

    /// Creates a new wrapper that rejects untracked storage and bytecode reads.
    pub fn with_partial_state_tracker(
        inner: StateProviderBox,
        is_tracked: impl Fn(&Address) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self { inner, partial_state_tracker: Some(std::sync::Arc::new(is_tracked)) }
    }

    fn is_tracked(&self, address: &Address) -> bool {
        self.partial_state_tracker.as_ref().is_none_or(|is_tracked| is_tracked(address))
    }
}

impl reth_storage_api::StateRootProvider for StateProviderTraitObjWrapper {
    fn state_root(
        &self,
        hashed_state: reth_trie::HashedPostState,
    ) -> reth_errors::ProviderResult<B256> {
        self.inner.state_root(hashed_state)
    }

    fn state_root_from_nodes(
        &self,
        input: reth_trie::TrieInput,
    ) -> reth_errors::ProviderResult<B256> {
        self.inner.state_root_from_nodes(input)
    }

    fn state_root_with_updates(
        &self,
        hashed_state: reth_trie::HashedPostState,
    ) -> reth_errors::ProviderResult<(B256, reth_trie::updates::TrieUpdates)> {
        self.inner.state_root_with_updates(hashed_state)
    }

    fn state_root_from_nodes_with_updates(
        &self,
        input: reth_trie::TrieInput,
    ) -> reth_errors::ProviderResult<(B256, reth_trie::updates::TrieUpdates)> {
        self.inner.state_root_from_nodes_with_updates(input)
    }
}

impl reth_storage_api::StorageRootProvider for StateProviderTraitObjWrapper {
    fn storage_root(
        &self,
        address: Address,
        hashed_storage: HashedStorage,
    ) -> ProviderResult<B256> {
        self.inner.storage_root(address, hashed_storage)
    }

    fn storage_proof(
        &self,
        address: Address,
        slot: B256,
        hashed_storage: HashedStorage,
    ) -> ProviderResult<reth_trie::StorageProof> {
        self.inner.storage_proof(address, slot, hashed_storage)
    }

    fn storage_multiproof(
        &self,
        address: Address,
        slots: &[B256],
        hashed_storage: HashedStorage,
    ) -> ProviderResult<reth_trie::StorageMultiProof> {
        self.inner.storage_multiproof(address, slots, hashed_storage)
    }
}

impl reth_storage_api::StateProofProvider for StateProviderTraitObjWrapper {
    fn proof(
        &self,
        input: reth_trie::TrieInput,
        address: Address,
        slots: &[B256],
    ) -> reth_errors::ProviderResult<reth_trie::AccountProof> {
        self.inner.proof(input, address, slots)
    }

    fn multiproof(
        &self,
        input: reth_trie::TrieInput,
        targets: MultiProofTargets,
    ) -> ProviderResult<reth_trie::MultiProof> {
        self.inner.multiproof(input, targets)
    }

    fn witness(
        &self,
        input: reth_trie::TrieInput,
        target: reth_trie::HashedPostState,
        mode: reth_trie::ExecutionWitnessMode,
    ) -> reth_errors::ProviderResult<Vec<alloy_primitives::Bytes>> {
        self.inner.witness(input, target, mode)
    }
}

impl reth_storage_api::AccountReader for StateProviderTraitObjWrapper {
    fn basic_account(
        &self,
        address: &Address,
    ) -> reth_errors::ProviderResult<Option<reth_primitives_traits::Account>> {
        let account = self.inner.basic_account(address)?;
        if !self.is_tracked(address) &&
            account
                .as_ref()
                .and_then(|account| account.bytecode_hash)
                .is_some_and(|code_hash| code_hash != KECCAK_EMPTY)
        {
            return Err(ProviderError::CodeNotTracked(*address))
        }
        Ok(account)
    }
}

impl reth_storage_api::BlockHashReader for StateProviderTraitObjWrapper {
    fn block_hash(
        &self,
        block_number: alloy_primitives::BlockNumber,
    ) -> reth_errors::ProviderResult<Option<B256>> {
        self.inner.block_hash(block_number)
    }

    fn convert_block_hash(
        &self,
        hash_or_number: alloy_rpc_types_eth::BlockHashOrNumber,
    ) -> reth_errors::ProviderResult<Option<B256>> {
        self.inner.convert_block_hash(hash_or_number)
    }

    fn canonical_hashes_range(
        &self,
        start: alloy_primitives::BlockNumber,
        end: alloy_primitives::BlockNumber,
    ) -> reth_errors::ProviderResult<Vec<B256>> {
        self.inner.canonical_hashes_range(start, end)
    }
}

impl HashedPostStateProvider for StateProviderTraitObjWrapper {
    fn hashed_post_state(&self, bundle_state: &BundleState) -> reth_trie::HashedPostState {
        self.inner.hashed_post_state(bundle_state)
    }
}

impl StateProvider for StateProviderTraitObjWrapper {
    fn storage(
        &self,
        account: Address,
        storage_key: alloy_primitives::StorageKey,
    ) -> reth_errors::ProviderResult<Option<alloy_primitives::StorageValue>> {
        if !self.is_tracked(&account) {
            return Err(ProviderError::StorageNotTracked(account))
        }
        self.inner.storage(account, storage_key)
    }

    fn account_code(
        &self,
        addr: &Address,
    ) -> reth_errors::ProviderResult<Option<reth_primitives_traits::Bytecode>> {
        if !self.is_tracked(addr) {
            return Err(ProviderError::CodeNotTracked(*addr))
        }
        self.inner.account_code(addr)
    }

    fn account_balance(&self, addr: &Address) -> reth_errors::ProviderResult<Option<U256>> {
        self.inner.account_balance(addr)
    }

    fn account_nonce(&self, addr: &Address) -> reth_errors::ProviderResult<Option<u64>> {
        self.inner.account_nonce(addr)
    }
}

impl BytecodeReader for StateProviderTraitObjWrapper {
    fn bytecode_by_hash(
        &self,
        code_hash: &B256,
    ) -> reth_errors::ProviderResult<Option<reth_primitives_traits::Bytecode>> {
        self.inner.bytecode_by_hash(code_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_primitives_traits::Account;
    use reth_storage_api::{
        AccountReader, BlockHashReader, StateProofProvider, StateRootProvider, StorageRootProvider,
    };
    use reth_trie::{
        updates::TrieUpdates, AccountProof, ExecutionWitnessMode, HashedPostState, HashedStorage,
        MultiProof, MultiProofTargets, StorageMultiProof, StorageProof, TrieInput,
    };
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct TestStateProvider {
        accounts: BTreeMap<Address, Account>,
        storage: BTreeMap<(Address, B256), U256>,
    }

    impl AccountReader for TestStateProvider {
        fn basic_account(&self, address: &Address) -> ProviderResult<Option<Account>> {
            Ok(self.accounts.get(address).cloned())
        }
    }

    impl BlockHashReader for TestStateProvider {
        fn block_hash(
            &self,
            _number: alloy_primitives::BlockNumber,
        ) -> ProviderResult<Option<B256>> {
            Ok(None)
        }

        fn canonical_hashes_range(
            &self,
            _start: alloy_primitives::BlockNumber,
            _end: alloy_primitives::BlockNumber,
        ) -> ProviderResult<Vec<B256>> {
            Ok(Vec::new())
        }
    }

    impl StateRootProvider for TestStateProvider {
        fn state_root(&self, _hashed_state: HashedPostState) -> ProviderResult<B256> {
            Ok(B256::ZERO)
        }

        fn state_root_from_nodes(&self, _input: TrieInput) -> ProviderResult<B256> {
            Ok(B256::ZERO)
        }

        fn state_root_with_updates(
            &self,
            _hashed_state: HashedPostState,
        ) -> ProviderResult<(B256, TrieUpdates)> {
            Ok((B256::ZERO, TrieUpdates::default()))
        }

        fn state_root_from_nodes_with_updates(
            &self,
            _input: TrieInput,
        ) -> ProviderResult<(B256, TrieUpdates)> {
            Ok((B256::ZERO, TrieUpdates::default()))
        }
    }

    impl StorageRootProvider for TestStateProvider {
        fn storage_root(
            &self,
            _address: Address,
            _hashed_storage: HashedStorage,
        ) -> ProviderResult<B256> {
            Ok(B256::ZERO)
        }

        fn storage_proof(
            &self,
            _address: Address,
            slot: B256,
            _hashed_storage: HashedStorage,
        ) -> ProviderResult<StorageProof> {
            Ok(StorageProof::new(slot))
        }

        fn storage_multiproof(
            &self,
            _address: Address,
            _slots: &[B256],
            _hashed_storage: HashedStorage,
        ) -> ProviderResult<StorageMultiProof> {
            Ok(StorageMultiProof::empty())
        }
    }

    impl StateProofProvider for TestStateProvider {
        fn proof(
            &self,
            _input: TrieInput,
            address: Address,
            _slots: &[B256],
        ) -> ProviderResult<AccountProof> {
            Ok(AccountProof::new(address))
        }

        fn multiproof(
            &self,
            _input: TrieInput,
            _targets: MultiProofTargets,
        ) -> ProviderResult<MultiProof> {
            Ok(MultiProof::default())
        }

        fn witness(
            &self,
            _input: TrieInput,
            _target: HashedPostState,
            _mode: ExecutionWitnessMode,
        ) -> ProviderResult<Vec<alloy_primitives::Bytes>> {
            Ok(Vec::new())
        }
    }

    impl HashedPostStateProvider for TestStateProvider {
        fn hashed_post_state(&self, _bundle_state: &BundleState) -> HashedPostState {
            HashedPostState::default()
        }
    }

    impl StateProvider for TestStateProvider {
        fn storage(
            &self,
            account: Address,
            storage_key: alloy_primitives::StorageKey,
        ) -> ProviderResult<Option<alloy_primitives::StorageValue>> {
            Ok(self.storage.get(&(account, storage_key)).copied())
        }
    }

    impl BytecodeReader for TestStateProvider {
        fn bytecode_by_hash(
            &self,
            _code_hash: &B256,
        ) -> ProviderResult<Option<reth_primitives_traits::Bytecode>> {
            Ok(None)
        }
    }

    #[test]
    fn partial_state_wrapper_rejects_untracked_storage() {
        let tracked = Address::with_last_byte(1);
        let untracked = Address::with_last_byte(2);
        let slot = B256::with_last_byte(3);
        let mut provider = TestStateProvider::default();
        provider.storage.insert((tracked, slot), U256::from(4));

        let wrapper = StateProviderTraitObjWrapper::with_partial_state_tracker(
            Box::new(provider),
            move |address| *address == tracked,
        );

        assert_eq!(wrapper.storage(tracked, slot).unwrap(), Some(U256::from(4)));

        let err = wrapper.storage(untracked, slot).unwrap_err();
        assert!(matches!(err, ProviderError::StorageNotTracked(address) if address == untracked));
    }

    #[test]
    fn partial_state_wrapper_rejects_untracked_contract_code() {
        let tracked = Address::with_last_byte(1);
        let untracked_contract = Address::with_last_byte(2);
        let untracked_eoa = Address::with_last_byte(3);
        let mut provider = TestStateProvider::default();
        provider.accounts.insert(
            untracked_contract,
            Account { nonce: 0, balance: U256::ZERO, bytecode_hash: Some(B256::with_last_byte(4)) },
        );
        provider
            .accounts
            .insert(untracked_eoa, Account { nonce: 0, balance: U256::ZERO, bytecode_hash: None });

        let wrapper = StateProviderTraitObjWrapper::with_partial_state_tracker(
            Box::new(provider),
            move |address| *address == tracked,
        );

        let err = wrapper.basic_account(&untracked_contract).unwrap_err();
        assert!(
            matches!(err, ProviderError::CodeNotTracked(address) if address == untracked_contract)
        );

        assert!(wrapper.basic_account(&untracked_eoa).unwrap().is_some());
    }
}
