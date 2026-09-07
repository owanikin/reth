use alloy_primitives::{BlockNumber, B256};

/// Durable metadata for the partial-state data currently stored in the database.
#[derive(Debug, Default, Eq, PartialEq, Clone, Copy)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[cfg_attr(any(test, feature = "reth-codec"), derive(reth_codecs::Compact))]
#[cfg_attr(any(test, feature = "reth-codec"), reth_codecs::add_arbitrary_tests(compact))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StoredPartialStateCheckpoint {
    /// Number of the block whose state is stored.
    pub block_number: BlockNumber,
    /// Hash of the block whose state is stored.
    pub block_hash: B256,
    /// State root represented by the partial-state tables.
    pub state_root: B256,
    /// Stable identity of the filter used to retain storage and bytecode.
    pub filter_hash: B256,
    /// Whether the state was fully downloaded and its root verified.
    pub sync_complete: bool,
}

#[cfg(any(test, feature = "reth-codec"))]
reth_codecs::impl_compression_for_compact!(StoredPartialStateCheckpoint);

/// Metadata required to validate and revert one journaled partial-state transition.
#[derive(Debug, Default, Eq, PartialEq, Clone, Copy)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[cfg_attr(any(test, feature = "reth-codec"), derive(reth_codecs::Compact))]
#[cfg_attr(any(test, feature = "reth-codec"), reth_codecs::add_arbitrary_tests(compact))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StoredPartialStateTransition {
    /// Hash of the block whose BAL produced this transition.
    pub block_hash: B256,
    /// Hash of the parent block.
    pub parent_block_hash: B256,
    /// Partial-state root before applying the transition.
    pub parent_state_root: B256,
    /// Verified partial-state root after applying the transition.
    pub state_root: B256,
}

#[cfg(any(test, feature = "reth-codec"))]
reth_codecs::impl_compression_for_compact!(StoredPartialStateTransition);
