use alloy_primitives::B256;

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
