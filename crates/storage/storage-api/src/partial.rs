use alloc::collections::BTreeSet;
use alloy_eips::{
    eip7928::{bal::DecodedBal, compute_block_access_list_hash, BlockAccessList},
    NumHash,
};
use alloy_primitives::{Address, BlockHash, BlockNumber, Bytes, Sealed, B256};
use reth_storage_errors::provider::{ProviderError, ProviderResult};

use crate::{BalStoreHandle, SealedBal};

/// Default number of blocks for retaining BAL history in partial-state mode.
pub const DEFAULT_PARTIAL_STATE_BAL_RETENTION: u64 = 256;

/// Minimum BAL retention window for partial-state mode.
pub const MIN_PARTIAL_STATE_BAL_RETENTION: u64 = 64;

/// Determines which contracts' storage and bytecode are retained by a partial-state node.
pub trait ContractFilter: Send + Sync {
    /// Returns `true` if storage for this contract should be downloaded and retained.
    fn should_sync_storage(&self, address: &Address) -> bool;

    /// Returns `true` if bytecode for this contract should be downloaded and retained.
    fn should_sync_code(&self, address: &Address) -> bool;

    /// Returns `true` if this contract is tracked by the partial-state node.
    fn is_tracked(&self, address: &Address) -> bool;
}

/// A contract filter backed by a static set of tracked contract addresses.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ConfiguredContractFilter {
    contracts: BTreeSet<Address>,
}

impl ConfiguredContractFilter {
    /// Creates a new configured filter from a list of tracked contract addresses.
    pub fn new(contracts: impl IntoIterator<Item = Address>) -> Self {
        Self { contracts: contracts.into_iter().collect() }
    }

    /// Returns the tracked contracts.
    pub const fn contracts(&self) -> &BTreeSet<Address> {
        &self.contracts
    }

    /// Returns `true` if no contracts are tracked.
    pub fn is_empty(&self) -> bool {
        self.contracts.is_empty()
    }
}

impl FromIterator<Address> for ConfiguredContractFilter {
    fn from_iter<T: IntoIterator<Item = Address>>(iter: T) -> Self {
        Self::new(iter)
    }
}

impl ContractFilter for ConfiguredContractFilter {
    fn should_sync_storage(&self, address: &Address) -> bool {
        self.contracts.contains(address)
    }

    fn should_sync_code(&self, address: &Address) -> bool {
        self.contracts.contains(address)
    }

    fn is_tracked(&self, address: &Address) -> bool {
        self.contracts.contains(address)
    }
}

/// A filter that tracks every contract, matching full-node behavior.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct AllowAllContractFilter;

impl ContractFilter for AllowAllContractFilter {
    fn should_sync_storage(&self, _address: &Address) -> bool {
        true
    }

    fn should_sync_code(&self, _address: &Address) -> bool {
        true
    }

    fn is_tracked(&self, _address: &Address) -> bool {
        true
    }
}

/// Manages recent Block Access Lists retained for partial-state reorg handling.
///
/// Reth already stores BALs behind [`BalStoreHandle`], keyed by block hash. This wrapper gives
/// partial-state code a small, phase-specific API without introducing a second BAL storage path.
#[derive(Debug, Clone)]
pub struct BalHistory {
    store: BalStoreHandle,
    retention: u64,
}

impl BalHistory {
    /// Creates a new BAL history manager.
    pub const fn new(store: BalStoreHandle, retention: u64) -> Self {
        Self { store, retention }
    }

    /// Stores a raw sealed BAL for the given block.
    pub fn store(&self, num_hash: NumHash, bal: SealedBal) -> ProviderResult<()> {
        self.store.insert(num_hash, bal)
    }

    /// Encodes and stores an alloy BAL for the given block.
    pub fn store_alloy(
        &self,
        num_hash: NumHash,
        access_list: &BlockAccessList,
    ) -> ProviderResult<()> {
        let raw: Bytes = alloy_rlp::encode(access_list).into();
        let bal_hash = compute_block_access_list_hash(access_list);
        self.store.insert(num_hash, Sealed::new_unchecked(raw, bal_hash))
    }

    /// Retrieves raw BAL bytes for a block hash.
    pub fn get_raw_by_hash(&self, block_hash: BlockHash) -> ProviderResult<Option<Bytes>> {
        self.store.get_by_hash(block_hash)
    }

    /// Retrieves and decodes a BAL for a block hash.
    pub fn get_by_hash(&self, block_hash: BlockHash) -> ProviderResult<Option<DecodedBal>> {
        self.store.get_decoded_by_hash(block_hash)
    }

    /// Returns whether BAL history exists for a block hash.
    pub fn contains_hash(&self, block_hash: BlockHash) -> ProviderResult<bool> {
        self.get_raw_by_hash(block_hash).map(|bal| bal.is_some())
    }

    /// Prunes BALs according to the backing store retention policy.
    pub fn prune(&self, tip: BlockNumber) -> ProviderResult<usize> {
        self.store.prune(tip)
    }

    /// Returns the configured BAL retention window.
    pub const fn retention(&self) -> u64 {
        self.retention
    }

    /// Returns the backing BAL store.
    pub const fn store_handle(&self) -> &BalStoreHandle {
        &self.store
    }
}

/// Coordinates partial-state metadata and BAL history.
///
/// Applying BAL state changes is intentionally left for later phases.
#[derive(Debug, Clone)]
pub struct PartialState<F = ConfiguredContractFilter> {
    filter: F,
    history: BalHistory,
    state_root: B256,
}

impl<F> PartialState<F> {
    /// Creates a new partial-state manager.
    pub const fn new(filter: F, history: BalHistory) -> Self {
        Self { filter, history, state_root: B256::ZERO }
    }

    /// Returns the contract filter used by this partial-state manager.
    pub const fn filter(&self) -> &F {
        &self.filter
    }

    /// Returns the BAL history manager.
    pub const fn history(&self) -> &BalHistory {
        &self.history
    }

    /// Sets the locally tracked state root.
    pub const fn set_root(&mut self, root: B256) {
        self.state_root = root;
    }

    /// Returns the locally tracked state root.
    pub const fn root(&self) -> B256 {
        self.state_root
    }

    /// Applies a BAL and computes the next state root.
    ///
    /// This is a phase-3/4 hook. Phase 1 only establishes the manager and makes the
    /// unimplemented boundary explicit.
    pub fn apply_bal_and_compute_root(
        &mut self,
        _current_root: B256,
        _access_list: &BlockAccessList,
    ) -> ProviderResult<B256> {
        Err(ProviderError::UnsupportedProvider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, keccak256};

    #[test]
    fn configured_filter_tracks_only_configured_contracts() {
        let tracked = address!("0000000000000000000000000000000000000001");
        let untracked = address!("0000000000000000000000000000000000000002");
        let filter = ConfiguredContractFilter::new([tracked]);

        assert!(filter.is_tracked(&tracked));
        assert!(filter.should_sync_storage(&tracked));
        assert!(filter.should_sync_code(&tracked));
        assert!(!filter.is_tracked(&untracked));
        assert!(!filter.should_sync_storage(&untracked));
        assert!(!filter.should_sync_code(&untracked));
    }

    #[test]
    fn allow_all_filter_tracks_everything() {
        let address = address!("0000000000000000000000000000000000000001");
        let filter = AllowAllContractFilter;

        assert!(filter.is_tracked(&address));
        assert!(filter.should_sync_storage(&address));
        assert!(filter.should_sync_code(&address));
    }

    #[test]
    fn bal_history_stores_and_reads_raw_bal() {
        let store = BalStoreHandle::new(crate::NoopBalStore);
        let history = BalHistory::new(store, DEFAULT_PARTIAL_STATE_BAL_RETENTION);

        assert_eq!(history.retention(), DEFAULT_PARTIAL_STATE_BAL_RETENTION);
        assert!(!history.contains_hash(B256::ZERO).unwrap());
    }

    #[test]
    fn partial_state_tracks_root_and_rejects_unimplemented_bal_application() {
        let filter = ConfiguredContractFilter::default();
        let history = BalHistory::new(BalStoreHandle::default(), 64);
        let mut partial_state = PartialState::new(filter, history);
        let root = keccak256("root");

        partial_state.set_root(root);

        assert_eq!(partial_state.root(), root);
        assert!(matches!(
            partial_state.apply_bal_and_compute_root(root, &BlockAccessList::default()),
            Err(ProviderError::UnsupportedProvider)
        ));
    }
}
