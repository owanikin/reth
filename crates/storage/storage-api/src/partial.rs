use alloc::{
    collections::{BTreeMap, BTreeSet},
    vec::Vec,
};
use alloy_eips::{
    eip7928::{bal::DecodedBal, compute_block_access_list_hash, AccountChanges, BlockAccessList},
    BlockNumHash, NumHash,
};
use alloy_primitives::{keccak256, Address, BlockHash, BlockNumber, Bytes, Sealed, B256, U256};
use auto_impl::auto_impl;
use reth_storage_errors::provider::{ProviderError, ProviderResult};
use reth_trie_common::TrieAccount;

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

    /// Returns `true` if storage for the account hash should be downloaded and retained.
    ///
    /// Snap sync range responses identify accounts by `keccak256(address)`, so partial-state
    /// filtering needs hash-based checks before the account address is available.
    fn should_sync_storage_by_hash(&self, account_hash: &B256) -> bool;

    /// Returns `true` if bytecode for the account hash should be downloaded and retained.
    ///
    /// Snap sync range responses identify accounts by `keccak256(address)`, so partial-state
    /// filtering needs hash-based checks before the account address is available.
    fn should_sync_code_by_hash(&self, account_hash: &B256) -> bool;

    /// Returns `true` if this contract is tracked by the partial-state node.
    fn is_tracked(&self, address: &Address) -> bool;
}

/// Writes snap state records retained by partial-state sync.
///
/// Snap state responses identify accounts and storage slots by their trie hashes. Implementations
/// should therefore persist these records into hash-keyed state tables unless they have a separate
/// source for address and storage preimages.
pub trait PartialStateSnapWriter {
    /// Writer error type.
    type Error;

    /// Persists an account leaf returned by snap sync, including its storage-root commitment.
    fn write_account(
        &mut self,
        account_hash: B256,
        account: TrieAccount,
    ) -> Result<(), Self::Error>;

    /// Persists a storage slot returned by snap sync.
    fn write_storage(
        &mut self,
        account_hash: B256,
        slot_hash: B256,
        value: U256,
    ) -> Result<(), Self::Error>;

    /// Persists bytecode returned by snap sync.
    fn write_bytecode(&mut self, code_hash: B256, bytecode: &[u8]) -> Result<(), Self::Error>;
}

/// Account data served to snap peers from hash-keyed state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialStateSnapAccount {
    /// Hash of the account address, also used as the account trie path.
    pub hash: B256,
    /// Account encoded in trie-account form by the network layer.
    pub account: TrieAccount,
}

/// Account range data served to snap peers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartialStateSnapAccountRange {
    /// Consecutive account leaves.
    pub accounts: Vec<PartialStateSnapAccount>,
    /// Boundary proof nodes for the range.
    pub proof: Vec<Bytes>,
}

/// Storage slot data served to snap peers from hash-keyed state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartialStateSnapStorage {
    /// Hash of the storage slot key, also used as the storage trie path.
    pub hash: B256,
    /// Storage value.
    pub value: U256,
}

/// Storage ranges data served to snap peers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartialStateSnapStorageRanges {
    /// Consecutive storage leaves, one list per requested account.
    pub slots: Vec<Vec<PartialStateSnapStorage>>,
    /// Boundary proof nodes for the final partial storage range.
    pub proof: Vec<Bytes>,
}

/// Bytecodes served to snap peers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartialStateSnapByteCodes {
    /// Bytecodes in request order, skipping hashes that are unavailable locally.
    pub codes: Vec<Bytes>,
}

/// Trie path requested by a snap peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialStateSnapTriePath {
    /// Path in the account trie.
    pub account_path: Bytes,
    /// Paths in the storage trie.
    pub slot_paths: Vec<Bytes>,
}

/// Trie nodes served to snap peers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartialStateSnapTrieNodes {
    /// Requested trie nodes.
    pub nodes: Vec<Bytes>,
}

/// Persisted state version available for snap serving.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartialStateSnapPivot {
    /// Number of the persisted block.
    pub block_number: BlockNumber,
    /// Hash of the persisted block.
    pub block_hash: BlockHash,
    /// State root committed to by the persisted block.
    pub state_root: B256,
}

/// Reads snap state records that can be served to peers.
#[auto_impl(&, Arc, Box)]
pub trait PartialStateSnapProvider: Send + Sync {
    /// Returns the persisted state version that this provider can serve over snap.
    fn snap_state_pivot(&self) -> ProviderResult<PartialStateSnapPivot> {
        Err(ProviderError::UnsupportedProvider)
    }

    /// Returns a snap account range for the requested state root.
    fn snap_account_range(
        &self,
        root_hash: B256,
        starting_hash: B256,
        limit_hash: B256,
        response_bytes: u64,
    ) -> ProviderResult<PartialStateSnapAccountRange>;

    /// Returns snap storage ranges for the requested account hashes.
    fn snap_storage_ranges(
        &self,
        root_hash: B256,
        account_hashes: &[B256],
        starting_hash: B256,
        limit_hash: B256,
        response_bytes: u64,
    ) -> ProviderResult<PartialStateSnapStorageRanges>;

    /// Returns bytecodes for the requested code hashes.
    fn snap_bytecodes(
        &self,
        hashes: &[B256],
        response_bytes: u64,
    ) -> ProviderResult<PartialStateSnapByteCodes>;

    /// Returns trie nodes for the requested paths.
    fn snap_trie_nodes(
        &self,
        root_hash: B256,
        paths: &[PartialStateSnapTriePath],
        response_bytes: u64,
    ) -> ProviderResult<PartialStateSnapTrieNodes>;
}

/// Computes a state root while accounting for intentionally omitted contract storage.
#[auto_impl(&, Arc, Box)]
pub trait PartialStateRootProvider: Send + Sync {
    /// Computes the account trie root for the current partial state.
    ///
    /// Storage roots for tracked accounts are computed from locally retained slots. Untracked
    /// accounts use their preserved storage-root commitments when available.
    fn partial_state_root(&self, filter: &dyn ContractFilter) -> ProviderResult<B256>;
}

/// Post-state account commitments resolved against a transition's expected state root.
///
/// Partial-state nodes cannot derive a new storage root for an untracked contract from BAL slot
/// values alone because they intentionally do not retain that contract's storage trie. Callers
/// resolve those account leaves before opening the state transition. A `None` value represents a
/// proof that the account is absent from the post-state.
pub type PartialStateResolvedAccounts = BTreeMap<Address, Option<TrieAccount>>;

/// Inputs required to apply one BAL to persisted partial state.
#[derive(Debug, Clone, Copy)]
pub struct PartialStateTransition<'a> {
    /// Number and hash of the block whose BAL produces this transition.
    pub block: BlockNumHash,
    /// Hash of the parent block.
    pub parent_block_hash: BlockHash,
    /// State root that the local partial state must have before applying the BAL.
    pub parent_root: B256,
    /// State root committed to by the child block.
    pub expected_root: B256,
    /// BAL hash committed to by the child block.
    pub expected_bal_hash: B256,
    /// Decoded block access list containing post-state values.
    pub access_list: &'a [AccountChanges],
    /// Verified post-state account leaves needed for untracked storage changes.
    pub resolved_accounts: &'a PartialStateResolvedAccounts,
}

/// Applies BAL state changes while retaining only configured storage and bytecode.
#[auto_impl(&, Arc, Box)]
pub trait PartialStateTransitionProvider: Send + Sync {
    /// Applies a transition atomically and returns the verified child state root.
    fn apply_partial_state_transition(
        &self,
        transition: PartialStateTransition<'_>,
        filter: &dyn ContractFilter,
    ) -> ProviderResult<B256>;

    /// Reverts the journaled transition for `block` and returns its restored parent pivot.
    fn revert_partial_state_transition(
        &self,
        block: BlockNumHash,
        filter: &dyn ContractFilter,
    ) -> ProviderResult<PartialStateSnapPivot>;

    /// Removes transition journals for blocks strictly below `block_number`.
    fn prune_partial_state_transition_journal(
        &self,
        block_number: BlockNumber,
    ) -> ProviderResult<usize>;

    /// Clears partial state and its transition journals before a full partial-state resync.
    fn reset_partial_state(&self) -> ProviderResult<()>;
}

/// A contract filter backed by a static set of tracked contract addresses.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ConfiguredContractFilter {
    contracts: BTreeSet<Address>,
    contract_hashes: BTreeSet<B256>,
}

impl ConfiguredContractFilter {
    /// Creates a new configured filter from a list of tracked contract addresses.
    pub fn new(contracts: impl IntoIterator<Item = Address>) -> Self {
        let contracts = contracts.into_iter().collect::<BTreeSet<_>>();
        let contract_hashes = contracts.iter().map(keccak256).collect();

        Self { contracts, contract_hashes }
    }

    /// Returns the tracked contracts.
    pub const fn contracts(&self) -> &BTreeSet<Address> {
        &self.contracts
    }

    /// Returns `true` if no contracts are tracked.
    pub fn is_empty(&self) -> bool {
        self.contracts.is_empty()
    }

    /// Returns the precomputed account hashes for tracked contracts.
    pub const fn contract_hashes(&self) -> &BTreeSet<B256> {
        &self.contract_hashes
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

    fn should_sync_storage_by_hash(&self, account_hash: &B256) -> bool {
        self.contract_hashes.contains(account_hash)
    }

    fn should_sync_code_by_hash(&self, account_hash: &B256) -> bool {
        self.contract_hashes.contains(account_hash)
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

    fn should_sync_storage_by_hash(&self, _account_hash: &B256) -> bool {
        true
    }

    fn should_sync_code_by_hash(&self, _account_hash: &B256) -> bool {
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

    /// Returns `true` if the BAL hashes to the expected block header commitment.
    pub fn validate_bal_hash(access_list: &BlockAccessList, expected_hash: B256) -> bool {
        compute_block_access_list_hash(access_list) == expected_hash
    }

    /// Stores the canonical BAL for a block and advances the locally tracked state root.
    ///
    /// Reth's engine validates and executes BAL-carrying payloads before they reach this helper.
    /// This method records the already-accepted BAL in the configured history store and tracks the
    /// resulting state root for partial-state bookkeeping.
    pub fn record_canonical_bal(
        &mut self,
        num_hash: NumHash,
        access_list: &BlockAccessList,
        state_root: B256,
    ) -> ProviderResult<()> {
        self.history.store_alloy(num_hash, access_list)?;
        self.state_root = state_root;
        Ok(())
    }

    /// Resets the locally tracked root to a known ancestor during reorg handling.
    ///
    /// Applying the new canonical branch still belongs to the engine/provider layer, which has the
    /// block bodies, decoded BALs, and trie-root machinery needed to validate each block.
    pub const fn reset_to_ancestor_root(&mut self, ancestor_root: B256) {
        self.state_root = ancestor_root;
    }

    /// Returns an unsupported-provider error.
    ///
    /// State mutation requires a provider-backed [`PartialStateTransitionProvider`]; this metadata
    /// container does not own a database transaction.
    pub const fn apply_bal_and_compute_root(
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
    fn configured_filter_matches_contract_hashes() {
        let tracked = address!("0000000000000000000000000000000000000001");
        let untracked = address!("0000000000000000000000000000000000000002");
        let filter = ConfiguredContractFilter::new([tracked]);

        let tracked_hash = keccak256(tracked);
        let untracked_hash = keccak256(untracked);

        assert!(filter.contract_hashes().contains(&tracked_hash));
        assert_eq!(
            filter.should_sync_storage(&tracked),
            filter.should_sync_storage_by_hash(&tracked_hash)
        );
        assert_eq!(
            filter.should_sync_code(&tracked),
            filter.should_sync_code_by_hash(&tracked_hash)
        );
        assert!(!filter.should_sync_storage_by_hash(&untracked_hash));
        assert!(!filter.should_sync_code_by_hash(&untracked_hash));
    }

    #[test]
    fn empty_configured_filter_rejects_contract_hashes() {
        let filter = ConfiguredContractFilter::default();
        let account_hash = keccak256(address!("0000000000000000000000000000000000000001"));

        assert!(filter.contract_hashes().is_empty());
        assert!(!filter.should_sync_storage_by_hash(&account_hash));
        assert!(!filter.should_sync_code_by_hash(&account_hash));
    }

    #[test]
    fn allow_all_filter_tracks_everything() {
        let address = address!("0000000000000000000000000000000000000001");
        let filter = AllowAllContractFilter;

        assert!(filter.is_tracked(&address));
        assert!(filter.should_sync_storage(&address));
        assert!(filter.should_sync_code(&address));
        assert!(filter.should_sync_storage_by_hash(&keccak256(address)));
        assert!(filter.should_sync_code_by_hash(&keccak256(address)));
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

    #[test]
    fn partial_state_validates_bal_hash() {
        let access_list = BlockAccessList::default();
        let hash = compute_block_access_list_hash(&access_list);

        assert!(PartialState::<ConfiguredContractFilter>::validate_bal_hash(&access_list, hash));
        assert!(!PartialState::<ConfiguredContractFilter>::validate_bal_hash(
            &access_list,
            B256::ZERO
        ));
    }

    #[test]
    fn partial_state_records_canonical_bal_and_root() {
        let filter = ConfiguredContractFilter::default();
        let history = BalHistory::new(BalStoreHandle::default(), 64);
        let mut partial_state = PartialState::new(filter, history);
        let root = keccak256("new root");
        let num_hash = NumHash::new(1, keccak256("block"));
        let access_list = BlockAccessList::default();

        partial_state.record_canonical_bal(num_hash, &access_list, root).unwrap();

        assert_eq!(partial_state.root(), root);
    }

    #[test]
    fn partial_state_resets_to_ancestor_root() {
        let filter = ConfiguredContractFilter::default();
        let history = BalHistory::new(BalStoreHandle::default(), 64);
        let mut partial_state = PartialState::new(filter, history);
        let head = keccak256("head");
        let ancestor = keccak256("ancestor");

        partial_state.set_root(head);
        partial_state.reset_to_ancestor_root(ancestor);

        assert_eq!(partial_state.root(), ancestor);
    }
}
