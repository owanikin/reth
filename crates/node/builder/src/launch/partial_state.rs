use alloy_consensus::BlockHeader;
use alloy_eips::{BlockNumHash, NumHash};
use alloy_primitives::{keccak256, Bytes, Sealed, B256};
#[cfg(test)]
use reth_chain_state::CanonStateNotification;
use reth_chainspec::{ChainSpecProvider, EthereumHardforks};
use reth_downloaders::snap::{resolve_partial_state_accounts, PartialStateAccountResolverError};
use reth_network_p2p::{error::RequestError, snap::client::SnapClient};
#[cfg(test)]
use reth_primitives_traits::NodePrimitives;
use reth_primitives_traits::SealedHeader;
#[cfg(test)]
use reth_provider::BlockNumReader;
use reth_provider::{providers::ProviderNodeTypes, HeaderProvider, ProviderFactory};
use reth_storage_api::{
    errors::provider::{PartialStateTransitionError, ProviderError},
    BalProvider, BalStoreHandle, ConfiguredContractFilter, DatabaseProviderFactory,
    PartialStateCheckpointProvider, PartialStateSnapPivot, PartialStateSnapProvider,
    PartialStateTransition, PartialStateTransitionProvider,
};
use reth_tracing::tracing::{debug, info, warn};
use std::time::Duration;
mod forkchoice;
pub(crate) use forkchoice::PartialStateAdvancer;

/// A bootstrap decision that does not modify the saved checkpoint or partial tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PartialStateBootstrap {
    /// Continue using a complete checkpoint, including journal-based reorg recovery.
    Resume(PartialStateSnapPivot),
    /// Download and verify a new snapshot before advancing with BALs.
    Sync(PartialStateSnapPivot),
}

/// Selects a snapshot whose successors can be processed with BALs. `None` means persistence or
/// fork activation has not caught up yet; callers must preserve the previous checkpoint and wait.
pub(crate) fn select_partial_state_bootstrap<N: ProviderNodeTypes>(
    provider_factory: &ProviderFactory<N>,
    canonical_provider: &impl HeaderProvider,
    filter: &ConfiguredContractFilter,
    resume_checkpoint: bool,
) -> eyre::Result<Option<PartialStateBootstrap>> {
    let provider = provider_factory.database_provider_ro()?;
    if resume_checkpoint &&
        let Some(checkpoint) = provider_factory.partial_state_checkpoint()? &&
        checkpoint.is_complete_for(filter) &&
        bal_compatible_pivot(canonical_provider, provider.chain_spec(), checkpoint.pivot)?
    {
        return Ok(Some(PartialStateBootstrap::Resume(checkpoint.pivot)))
    }

    // Select from a database snapshot, not the in-memory tip: the snap server must be able to
    // serve the persisted state version. Do not select an orphaned persisted pivot during a reorg.
    let pivot = provider.snap_state_pivot()?;
    if canonical_provider
        .sealed_header(pivot.block_number)?
        .is_none_or(|header| header.hash() != pivot.block_hash)
    {
        return Ok(None)
    }
    if bal_compatible_pivot(canonical_provider, provider.chain_spec(), pivot)? {
        Ok(Some(PartialStateBootstrap::Sync(pivot)))
    } else {
        Ok(None)
    }
}

/// Polls persistence independently of canonical notifications, which may stop while it catches up.
pub(crate) async fn wait_for_partial_state_bootstrap<N: ProviderNodeTypes>(
    provider_factory: &ProviderFactory<N>,
    canonical_provider: &impl HeaderProvider,
    filter: &ConfiguredContractFilter,
    resume_checkpoint: bool,
) -> eyre::Result<PartialStateBootstrap> {
    let mut waiting = false;
    loop {
        if let Some(bootstrap) = select_partial_state_bootstrap(
            provider_factory,
            canonical_provider,
            filter,
            resume_checkpoint,
        )? {
            return Ok(bootstrap)
        }
        if !waiting {
            let checkpoint = provider_factory.partial_state_checkpoint()?;
            info!(target: "reth::cli", checkpoint = ?checkpoint.map(|checkpoint| checkpoint.pivot),
                "Waiting for a persisted BAL-compatible partial-state pivot");
            waiting = true;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

fn bal_compatible_pivot(
    provider: &impl HeaderProvider,
    chain_spec: &impl EthereumHardforks,
    pivot: PartialStateSnapPivot,
) -> eyre::Result<bool> {
    let Some(header) = provider.sealed_header_by_hash(pivot.block_hash)? else { return Ok(false) };
    eyre::ensure!(
        header.number() == pivot.block_number && header.state_root() == pivot.state_root,
        "partial-state pivot does not match its header: {} ({})",
        pivot.block_number,
        pivot.block_hash
    );
    if chain_spec.is_amsterdam_active_at_timestamp(header.timestamp()) {
        eyre::ensure!(
            header.number() == 0 || header.block_access_list_hash().is_some(),
            "BAL-active partial-state pivot {} has no block access list commitment",
            pivot.block_number
        );
        return Ok(true)
    }

    // A last pre-fork state is also sufficient, but only once its immediate canonical child is
    // known to be BAL-bearing. Never infer eligibility from wall-clock time or a distant tip.
    let Some(child_number) = pivot.block_number.checked_add(1) else { return Ok(false) };
    let Some(child) = provider.sealed_header(child_number)? else { return Ok(false) };
    if child.parent_hash() != pivot.block_hash ||
        !chain_spec.is_amsterdam_active_at_timestamp(child.timestamp())
    {
        return Ok(false)
    }
    eyre::ensure!(
        child.block_access_list_hash().is_some(),
        "BAL-active canonical block {child_number} has no block access list commitment"
    );
    Ok(true)
}

fn commitment_is_unavailable(error: &eyre::Report) -> bool {
    error.downcast_ref::<PartialStateAccountResolverError>().is_some_and(|error| match error {
        PartialStateAccountResolverError::UnprovenAccountAbsence { .. } => true,
        PartialStateAccountResolverError::Request(error) => {
            error.is_retryable() || matches!(error, RequestError::UnsupportedCapability)
        }
        _ => false,
    })
}

/// Retains a raw payload BAL until the block becomes canonical.
pub(crate) fn retain_payload_bal(store: &BalStoreHandle, num_hash: NumHash, raw: &Bytes) {
    let bal = Sealed::new_unchecked(raw.clone(), keccak256(raw));
    if let Err(err) = store.insert(num_hash, bal) {
        warn!(
            target: "reth::cli",
            block_number = num_hash.number,
            block_hash = %num_hash.hash,
            %err,
            "Failed to retain payload BAL"
        );
    }
}

/// Result of reconciling partial state with the current canonical chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PartialStateAdvanceOutcome {
    /// The verified head is preserved; the driver will retry against the current canonical chain.
    AwaitingCommitment,
    /// The selected branch is too long to buffer; a newer persisted pivot is required.
    TargetTooDistant,
    /// Replay reached a legitimate pre-BAL block; bootstrap from a newer verified snapshot.
    BootstrapRequired {
        /// The block whose transition cannot be reconstructed from a BAL.
        pre_bal_block: BlockNumHash,
    },
    /// Partial state reached the canonical tip.
    Reconciled {
        /// Number of canonical BAL transitions applied.
        advanced: u64,
        /// Number of non-canonical transitions reverted.
        reverted: u64,
        /// Number of expired transition journals removed.
        pruned: usize,
    },
    /// A required rollback journal is unavailable, so snap sync must rebuild partial state.
    ResyncRequired {
        /// First block that could not be reverted.
        unavailable_block: BlockNumHash,
        /// Number of newer transitions reverted before the gap was reached.
        reverted: u64,
    },
}

/// Header and BAL view used by replay, without requiring locally executed blocks.
trait PartialStateChain: Sync {
    type Header: reth_primitives_traits::BlockHeader;
    fn tip(&self) -> eyre::Result<BlockNumHash>;
    fn hash(&self, number: u64) -> eyre::Result<Option<B256>>;
    fn replay_header(&self, number: u64) -> eyre::Result<Option<SealedHeader<Self::Header>>>;
    fn oldest_block(&self) -> u64 {
        0
    }
    fn prepare_bal(
        &self,
        _block: BlockNumHash,
        _expected: B256,
    ) -> impl std::future::Future<Output = eyre::Result<()>> + Send {
        async { Ok(()) }
    }
}

#[cfg(test)]
impl<T: HeaderProvider + BlockNumReader + Sync> PartialStateChain for T {
    type Header = T::Header;
    fn tip(&self) -> eyre::Result<BlockNumHash> {
        let number = self.best_block_number()?;
        Ok(self.sealed_header(number)?.ok_or_else(|| eyre::eyre!("missing tip"))?.num_hash())
    }
    fn hash(&self, number: u64) -> eyre::Result<Option<B256>> {
        Ok(self.sealed_header(number)?.map(|header| header.hash()))
    }
    fn replay_header(&self, number: u64) -> eyre::Result<Option<SealedHeader<Self::Header>>> {
        Ok(self.sealed_header(number)?)
    }
}

/// Reconciles verified partial state with the current canonical chain using retained BALs.
async fn advance_partial_state_to_target<N, Client>(
    client: &Client,
    provider_factory: &ProviderFactory<N>,
    canonical_provider: &impl PartialStateChain,
    filter: &ConfiguredContractFilter,
    head: &mut PartialStateSnapPivot,
    notified_target: BlockNumHash,
    retention: u64,
) -> eyre::Result<PartialStateAdvanceOutcome>
where
    N: ProviderNodeTypes + 'static,
    Client: SnapClient,
{
    let mut advanced = 0;
    let mut reverted = 0;
    let mut observed_canonical_tip = None;
    loop {
        // Cached headers and BALs may replay without a network yield. Let forkchoice updates
        // and shutdown run between atomic transitions, including during a long rollback.
        tokio::task::yield_now().await;
        let canonical_tip = canonical_provider.tip()?;
        if canonical_tip != notified_target && observed_canonical_tip != Some(canonical_tip) {
            debug!(
                target: "reth::cli",
                notified_block = notified_target.number,
                notified_hash = %notified_target.hash,
                canonical_block = canonical_tip.number,
                canonical_hash = %canonical_tip.hash,
                "Reconciling partial state to current canonical tip"
            );
        }
        observed_canonical_tip = Some(canonical_tip);

        let canonical_head = canonical_provider.hash(head.block_number)?;
        let head_is_canonical =
            canonical_head == Some(head.block_hash) && head.block_number <= canonical_tip.number;
        if !head_is_canonical {
            if head.block_number < canonical_provider.oldest_block() ||
                (head.block_number == canonical_provider.oldest_block() &&
                    head.block_number != 0)
            {
                return Ok(PartialStateAdvanceOutcome::ResyncRequired {
                    unavailable_block: BlockNumHash::new(head.block_number, head.block_hash),
                    reverted,
                })
            }
            if head.block_number == 0 {
                eyre::bail!("partial-state genesis {} is not canonical", head.block_hash)
            }
            let block = BlockNumHash::new(head.block_number, head.block_hash);
            match provider_factory.revert_partial_state_transition(block, filter) {
                Ok(parent) => {
                    info!(
                        target: "reth::cli",
                        block_number = block.number,
                        block_hash = %block.hash,
                        parent_number = parent.block_number,
                        parent_hash = %parent.block_hash,
                        state_root = %parent.state_root,
                        "Reverted non-canonical partial state with BAL journal"
                    );
                    *head = parent;
                    reverted += 1;
                    continue
                }
                Err(ProviderError::PartialStateTransition(error))
                    if matches!(
                        error.as_ref(),
                        PartialStateTransitionError::JournalNotFound { .. } |
                            PartialStateTransitionError::JournalBlockHashMismatch { .. }
                    ) =>
                {
                    return Ok(PartialStateAdvanceOutcome::ResyncRequired {
                        unavailable_block: block,
                        reverted,
                    })
                }
                Err(error) => return Err(error.into()),
            }
        }

        if head.block_number == canonical_tip.number {
            eyre::ensure!(
                head.block_hash == canonical_tip.hash,
                "partial-state head {} disagrees with canonical tip {} at block {}",
                head.block_hash,
                canonical_tip.hash,
                head.block_number
            );
            let retain_from = head.block_number.saturating_add(1).saturating_sub(retention.max(1));
            let pruned = provider_factory.prune_partial_state_transition_journal(retain_from)?;
            return Ok(PartialStateAdvanceOutcome::Reconciled { advanced, reverted, pruned })
        }

        let number = head.block_number + 1;
        let header = canonical_provider
            .replay_header(number)?
            .ok_or_else(|| eyre::eyre!("canonical header {number} is unavailable"))?;
        if header.parent_hash() != head.block_hash {
            continue
        }

        let block_hash = header.hash();
        let Some(expected_bal_hash) = header.block_access_list_hash() else {
            if !provider_factory.chain_spec().is_amsterdam_active_at_timestamp(header.timestamp()) {
                return Ok(PartialStateAdvanceOutcome::BootstrapRequired {
                    pre_bal_block: header.num_hash(),
                })
            }
            eyre::bail!(
                "canonical block {number} ({block_hash}) has no block access list commitment"
            )
        };
        canonical_provider.prepare_bal(header.num_hash(), expected_bal_hash).await?;
        let decoded_bal =
            provider_factory.bal_store().get_decoded_by_hash(block_hash)?.ok_or_else(|| {
                eyre::eyre!(
                    "block access list for canonical block {number} ({block_hash}) is unavailable"
                )
            })?;
        decoded_bal.ensure_hash(expected_bal_hash)?;

        let resolved_accounts = resolve_partial_state_accounts(
            client,
            header.state_root(),
            decoded_bal.as_bal(),
            filter,
        )
        .await?;
        // Network requests yield long enough for forkchoice to replace the target branch.
        if canonical_provider.tip()?.number < number ||
            canonical_provider.hash(number)? != Some(block_hash)
        {
            continue
        }
        let computed_root = provider_factory.apply_partial_state_transition(
            PartialStateTransition {
                block: BlockNumHash::new(number, block_hash),
                parent_block_hash: header.parent_hash(),
                parent_root: head.state_root,
                expected_root: header.state_root(),
                expected_bal_hash,
                access_list: decoded_bal.as_bal(),
                resolved_accounts: &resolved_accounts,
            },
            filter,
        )?;

        *head =
            PartialStateSnapPivot { block_number: number, block_hash, state_root: computed_root };
        advanced += 1;
        info!(
            target: "reth::cli",
            block_number = number,
            %block_hash,
            state_root = %computed_root,
            resolved_accounts = resolved_accounts.len(),
            "Advanced canonical partial state with BAL"
        );
    }
}

/// Applies one canonical-state notification without re-reading its branches from the provider.
///
/// Reorged blocks are removed in descending order before replacement blocks are applied in
/// ascending order. Only headers and retained BALs are consumed locally. A snap peer must serve
/// proofs at each replacement block's root; execution outcomes are not used to fill missing state.
#[cfg(test)]
async fn advance_partial_state_with_notification<N, Client>(
    client: &Client,
    provider_factory: &ProviderFactory<N>,
    filter: &ConfiguredContractFilter,
    head: &mut PartialStateSnapPivot,
    notification: CanonStateNotification<N::Primitives>,
    retention: u64,
) -> eyre::Result<PartialStateAdvanceOutcome>
where
    N: ProviderNodeTypes + 'static,
    N::Primitives: NodePrimitives,
    Client: SnapClient,
{
    let (reverted_chain, committed_chain) = match notification {
        CanonStateNotification::Commit { new } => (None, new),
        CanonStateNotification::Reorg { old, new } => (Some(old), new),
    };
    let target = if committed_chain.is_empty() {
        let fork = reverted_chain
            .as_ref()
            .ok_or_else(|| eyre::eyre!("empty canonical commit notification"))?
            .fork_block();
        BlockNumHash::new(fork.number, fork.hash)
    } else {
        committed_chain.tip().num_hash()
    };

    let notification_tip =
        reverted_chain.as_ref().map_or(target.number, |old| old.tip().number().max(target.number));
    if head.block_number > notification_tip ||
        (head.block_number == target.number && head.block_hash == target.hash)
    {
        return Ok(PartialStateAdvanceOutcome::Reconciled { advanced: 0, reverted: 0, pruned: 0 })
    }

    let mut advanced = 0;
    let mut reverted = 0;
    if let Some(old) = reverted_chain {
        let fork = old.fork_block();
        for block in old.blocks().values().rev() {
            if block.number() > head.block_number {
                continue
            }
            if head.block_number == fork.number && head.block_hash == fork.hash {
                break
            }
            let block = block.num_hash();
            eyre::ensure!(
                block.number == head.block_number && block.hash == head.block_hash,
                "partial-state head {} ({}) does not match reverted notification block {} ({})",
                head.block_number,
                head.block_hash,
                block.number,
                block.hash
            );
            match provider_factory.revert_partial_state_transition(block, filter) {
                Ok(parent) => {
                    info!(
                        target: "reth::cli",
                        block_number = block.number,
                        block_hash = %block.hash,
                        parent_number = parent.block_number,
                        parent_hash = %parent.block_hash,
                        state_root = %parent.state_root,
                        "Reverted non-canonical partial state from canonical notification"
                    );
                    *head = parent;
                    reverted += 1;
                }
                Err(ProviderError::PartialStateTransition(error))
                    if matches!(
                        error.as_ref(),
                        PartialStateTransitionError::JournalNotFound { .. } |
                            PartialStateTransitionError::JournalBlockHashMismatch { .. }
                    ) =>
                {
                    return Ok(PartialStateAdvanceOutcome::ResyncRequired {
                        unavailable_block: block,
                        reverted,
                    })
                }
                Err(error) => return Err(error.into()),
            }
        }
        eyre::ensure!(
            head.block_number == fork.number && head.block_hash == fork.hash,
            "partial-state reorg stopped at {} ({}), expected fork block {} ({})",
            head.block_number,
            head.block_hash,
            fork.number,
            fork.hash
        );
    }

    if !committed_chain.is_empty() {
        for block in committed_chain.blocks().values() {
            let number = block.number();
            let block_hash = block.hash();
            if number < head.block_number {
                continue
            }
            if number == head.block_number {
                eyre::ensure!(
                    block_hash == head.block_hash,
                    "partial-state head {} disagrees with committed notification {} at block {}",
                    head.block_hash,
                    block_hash,
                    number
                );
                continue
            }
            eyre::ensure!(
                number == head.block_number + 1 && block.parent_hash() == head.block_hash,
                "committed notification block {} ({}) does not extend partial-state head {} ({})",
                number,
                block_hash,
                head.block_number,
                head.block_hash
            );

            let Some(expected_bal_hash) = block.block_access_list_hash() else {
                if !provider_factory
                    .chain_spec()
                    .is_amsterdam_active_at_timestamp(block.timestamp())
                {
                    return Ok(PartialStateAdvanceOutcome::BootstrapRequired {
                        pre_bal_block: block.num_hash(),
                    })
                }
                eyre::bail!(
                    "canonical block {number} ({block_hash}) has no block access list commitment"
                )
            };
            let decoded_bal = provider_factory
                .bal_store()
                .get_decoded_by_hash(block_hash)?
                .ok_or_else(|| {
                    eyre::eyre!(
                        "block access list for canonical block {number} ({block_hash}) is unavailable"
                    )
                })?;
            decoded_bal.ensure_hash(expected_bal_hash)?;

            let resolved_accounts = resolve_partial_state_accounts(
                client,
                block.state_root(),
                decoded_bal.as_bal(),
                filter,
            )
            .await?;
            let computed_root = provider_factory.apply_partial_state_transition(
                PartialStateTransition {
                    block: BlockNumHash::new(number, block_hash),
                    parent_block_hash: block.parent_hash(),
                    parent_root: head.state_root,
                    expected_root: block.state_root(),
                    expected_bal_hash,
                    access_list: decoded_bal.as_bal(),
                    resolved_accounts: &resolved_accounts,
                },
                filter,
            )?;

            *head = PartialStateSnapPivot {
                block_number: number,
                block_hash,
                state_root: computed_root,
            };
            advanced += 1;
            info!(
                target: "reth::cli",
                block_number = number,
                %block_hash,
                state_root = %computed_root,
                resolved_accounts = resolved_accounts.len(),
                "Advanced canonical partial state from canonical notification"
            );
        }
    }

    eyre::ensure!(
        head.block_number == target.number && head.block_hash == target.hash,
        "partial-state notification ended at {} ({}), expected {} ({})",
        head.block_number,
        head.block_hash,
        target.number,
        target.hash
    );
    let retain_from = head.block_number.saturating_add(1).saturating_sub(retention.max(1));
    let pruned = provider_factory.prune_partial_state_transition_journal(retain_from)?;
    Ok(PartialStateAdvanceOutcome::Reconciled { advanced, reverted, pruned })
}

#[cfg(test)]
mod tests {
    use super::*;
    mod forkchoice;
    use alloy_consensus::{
        constants::{EMPTY_ROOT_HASH, KECCAK_EMPTY},
        Header,
    };
    use alloy_eips::eip7928::{
        compute_block_access_list_hash, AccountChanges, BalanceChange, SlotChanges, StorageChange,
    };
    use alloy_primitives::{address, Address, B256, U256};
    use futures::future;
    use reth_chainspec::ChainSpecBuilder;
    use reth_db_api::{
        tables,
        transaction::{DbTx, DbTxMut},
    };
    use reth_eth_wire_types::snap::{
        AccountData, AccountRangeMessage, GetAccountRangeMessage, GetByteCodesMessage,
        GetStorageRangesMessage, GetTrieNodesMessage,
    };
    use reth_ethereum_primitives::{Block, BlockBody};
    use reth_execution_types::{Chain, ExecutionOutcome};
    use reth_network_p2p::{
        download::DownloadClient,
        error::{PeerRequestResult, RequestError},
        priority::Priority,
        snap::client::SnapResponse,
    };
    use reth_network_peers::{PeerId, WithPeerId};
    use reth_primitives_traits::{RecoveredBlock, SealedBlock, SealedHeader};
    use reth_provider::{
        providers::BlockchainProvider,
        test_utils::{
            create_test_provider_factory, create_test_provider_factory_with_chain_spec,
            MockEthProvider, MockNodeTypesWithDB,
        },
        StaticFileProviderFactory, StaticFileSegment, StaticFileWriter,
    };
    use reth_stages::{StageCheckpoint, StageId};
    use reth_storage_api::{
        BalHistory, DBProvider, DatabaseProviderFactory, PartialStateCheckpointProvider,
        PartialStateRootProvider, PartialStateSnapWriter, StageCheckpointWriter,
        DEFAULT_PARTIAL_STATE_BAL_RETENTION,
    };
    use reth_trie::root::{state_root_unhashed, state_root_unsorted, storage_root};
    use reth_trie_common::{proof::ProofRetainer, HashBuilder, Nibbles, TrieAccount};
    use std::{
        collections::{BTreeMap, VecDeque},
        sync::{Arc, Mutex},
    };

    #[derive(Debug, Clone, Copy)]
    struct NoRequestSnapClient;

    impl DownloadClient for NoRequestSnapClient {
        fn report_bad_message(&self, _peer_id: PeerId) {}

        fn num_connected_peers(&self) -> usize {
            1
        }
    }

    type NoRequestOutput = future::Ready<PeerRequestResult<SnapResponse>>;

    impl SnapClient for NoRequestSnapClient {
        type Output = NoRequestOutput;

        fn get_account_range_with_priority(
            &self,
            _request: GetAccountRangeMessage,
            _priority: Priority,
        ) -> Self::Output {
            future::ready(Err(RequestError::Timeout))
        }

        fn get_storage_ranges(&self, _request: GetStorageRangesMessage) -> Self::Output {
            future::ready(Err(RequestError::Timeout))
        }

        fn get_storage_ranges_with_priority(
            &self,
            _request: GetStorageRangesMessage,
            _priority: Priority,
        ) -> Self::Output {
            future::ready(Err(RequestError::Timeout))
        }

        fn get_byte_codes(&self, _request: GetByteCodesMessage) -> Self::Output {
            future::ready(Err(RequestError::Timeout))
        }

        fn get_byte_codes_with_priority(
            &self,
            _request: GetByteCodesMessage,
            _priority: Priority,
        ) -> Self::Output {
            future::ready(Err(RequestError::Timeout))
        }

        fn get_trie_nodes(&self, _request: GetTrieNodesMessage) -> Self::Output {
            future::ready(Err(RequestError::Timeout))
        }

        fn get_trie_nodes_with_priority(
            &self,
            _request: GetTrieNodesMessage,
            _priority: Priority,
        ) -> Self::Output {
            future::ready(Err(RequestError::Timeout))
        }
    }

    fn recovered_empty_block(header: Header, hash: B256) -> RecoveredBlock<Block> {
        RecoveredBlock::new_sealed(
            SealedBlock::from_sealed_parts(SealedHeader::new(header, hash), BlockBody::default()),
            Vec::new(),
        )
    }

    fn bootstrap_fixture() -> (ProviderFactory<MockNodeTypesWithDB>, Vec<SealedHeader>) {
        bootstrap_fixture_at(48)
    }

    fn bootstrap_fixture_at(
        activation: u64,
    ) -> (ProviderFactory<MockNodeTypesWithDB>, Vec<SealedHeader>) {
        let factory = create_test_provider_factory_with_chain_spec(Arc::new(
            ChainSpecBuilder::mainnet().with_amsterdam_at(activation).build(),
        ));
        let mut headers = Vec::new();
        let mut parent_hash = B256::ZERO;
        for (number, timestamp) in [0, 36, 42, 48, 54].into_iter().enumerate() {
            let header = SealedHeader::seal_slow(Header {
                number: number as u64,
                parent_hash,
                timestamp,
                state_root: EMPTY_ROOT_HASH,
                block_access_list_hash: (timestamp >= 48)
                    .then(|| compute_block_access_list_hash(&[])),
                ..Default::default()
            });
            parent_hash = header.hash();
            headers.push(header);
        }
        let files = factory.static_file_provider();
        let mut writer = files.latest_writer(StaticFileSegment::Headers).unwrap();
        for header in &headers {
            writer.append_header(header, &header.hash()).unwrap();
        }
        writer.commit().unwrap();
        drop(writer);
        let provider = factory.database_provider_rw().unwrap();
        for header in &headers {
            provider.tx_ref().put::<tables::HeaderNumbers>(header.hash(), header.number()).unwrap();
        }
        provider.save_stage_checkpoint(StageId::Finish, StageCheckpoint::new(0)).unwrap();
        provider.commit().unwrap();
        (factory, headers)
    }

    fn bootstrap_pivot(header: &SealedHeader) -> PartialStateSnapPivot {
        PartialStateSnapPivot {
            block_number: header.number(),
            block_hash: header.hash(),
            state_root: header.state_root(),
        }
    }

    fn persist_bootstrap_tip(factory: &ProviderFactory<MockNodeTypesWithDB>, number: u64) {
        let provider = factory.database_provider_rw().unwrap();
        provider.save_stage_checkpoint(StageId::Finish, StageCheckpoint::new(number)).unwrap();
        provider.commit().unwrap();
    }

    #[test]
    fn bootstrap_waits_for_persistence_without_reusing_pre_bal_checkpoint() {
        let (factory, headers) = bootstrap_fixture();
        let filter = ConfiguredContractFilter::new([]);
        let pivot = bootstrap_pivot(&headers[0]);
        assert_eq!(
            select_partial_state_bootstrap(&factory, &factory, &filter, true).unwrap(),
            None
        );
        factory.begin_partial_state_sync(pivot, &filter).unwrap();
        let checkpoint = factory.complete_partial_state_sync(pivot, &filter).unwrap();

        // A BAL-bearing tip exists, but persisted state and the saved checkpoint still have a gap.
        for persisted in [0, 1] {
            persist_bootstrap_tip(&factory, persisted);
            assert_eq!(
                select_partial_state_bootstrap(&factory, &factory, &filter, true).unwrap(),
                None
            );
            assert_eq!(factory.partial_state_checkpoint().unwrap(), Some(checkpoint));
            assert_eq!(factory.partial_state_root(&filter).unwrap(), pivot.state_root);
        }

        persist_bootstrap_tip(&factory, 3);
        assert_eq!(
            select_partial_state_bootstrap(&factory, &factory, &filter, true).unwrap(),
            Some(PartialStateBootstrap::Sync(bootstrap_pivot(&headers[3])))
        );
        // Selection alone must not replace the last verified state.
        assert_eq!(factory.partial_state_checkpoint().unwrap(), Some(checkpoint));
    }

    #[test]
    fn bootstrap_accepts_last_pre_bal_parent_and_resumes_compatible_checkpoint() {
        let (factory, headers) = bootstrap_fixture();
        let filter = ConfiguredContractFilter::new([]);
        persist_bootstrap_tip(&factory, 2);
        let pivot = bootstrap_pivot(&headers[2]);
        assert_eq!(
            select_partial_state_bootstrap(&factory, &factory, &filter, true).unwrap(),
            Some(PartialStateBootstrap::Sync(pivot))
        );
        factory.begin_partial_state_sync(pivot, &filter).unwrap();
        factory.complete_partial_state_sync(pivot, &filter).unwrap();
        persist_bootstrap_tip(&factory, 4);
        assert_eq!(
            select_partial_state_bootstrap(&factory, &factory, &filter, true).unwrap(),
            Some(PartialStateBootstrap::Resume(pivot))
        );
        assert_eq!(
            select_partial_state_bootstrap(&factory, &factory, &filter, false).unwrap(),
            Some(PartialStateBootstrap::Sync(bootstrap_pivot(&headers[4])))
        );
    }

    #[test]
    fn bootstrap_does_not_resume_incomplete_or_differently_filtered_state() {
        let (factory, headers) = bootstrap_fixture();
        let filter = ConfiguredContractFilter::new([]);
        let pivot = bootstrap_pivot(&headers[3]);
        persist_bootstrap_tip(&factory, 4);
        factory.begin_partial_state_sync(pivot, &filter).unwrap();
        let expected = Some(PartialStateBootstrap::Sync(bootstrap_pivot(&headers[4])));
        assert_eq!(
            select_partial_state_bootstrap(&factory, &factory, &filter, true).unwrap(),
            expected
        );
        factory.complete_partial_state_sync(pivot, &filter).unwrap();
        let changed = ConfiguredContractFilter::new([Address::repeat_byte(1)]);
        assert_eq!(
            select_partial_state_bootstrap(&factory, &factory, &changed, true).unwrap(),
            expected
        );
    }

    #[tokio::test(start_paused = true)]
    async fn bootstrap_wakes_after_persistence_without_new_notifications() {
        let (factory, headers) = bootstrap_fixture();
        let filter = ConfiguredContractFilter::new([]);
        let ready = wait_for_partial_state_bootstrap(&factory, &factory, &filter, true);
        tokio::pin!(ready);
        tokio::select! {
            biased;
            result = &mut ready => panic!("bootstrap should be waiting: {result:?}"),
            _ = tokio::task::yield_now() => {}
        }
        persist_bootstrap_tip(&factory, 3);
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(ready.await.unwrap(), PartialStateBootstrap::Sync(bootstrap_pivot(&headers[3])));
    }

    #[tokio::test]
    async fn bootstrap_required_for_pre_bal_replay_preserves_checkpoint() {
        let (factory, headers) = bootstrap_fixture();
        let filter = ConfiguredContractFilter::new([]);
        let mut head = bootstrap_pivot(&headers[0]);
        factory.begin_partial_state_sync(head, &filter).unwrap();
        let checkpoint = factory.complete_partial_state_sync(head, &filter).unwrap();
        persist_bootstrap_tip(&factory, 4);
        let expected =
            PartialStateAdvanceOutcome::BootstrapRequired { pre_bal_block: headers[1].num_hash() };
        assert_eq!(
            advance_partial_state_to_target(
                &NoRequestSnapClient,
                &factory,
                &factory,
                &filter,
                &mut head,
                headers[4].num_hash(),
                64,
            )
            .await
            .unwrap(),
            expected
        );
        let notification = CanonStateNotification::Commit {
            new: Arc::new(Chain::new(
                [recovered_empty_block(headers[1].clone_header(), headers[1].hash())],
                ExecutionOutcome::default(),
                BTreeMap::new(),
            )),
        };
        assert_eq!(
            advance_partial_state_with_notification(
                &NoRequestSnapClient,
                &factory,
                &filter,
                &mut head,
                notification,
                64,
            )
            .await
            .unwrap(),
            expected
        );
        assert_eq!(head, checkpoint.pivot);
        assert_eq!(factory.partial_state_checkpoint().unwrap(), Some(checkpoint));
        assert_eq!(factory.partial_state_root(&filter).unwrap(), checkpoint.pivot.state_root);
        assert_eq!(
            factory
                .database_provider_ro()
                .unwrap()
                .tx_ref()
                .entries::<tables::PartialStateTransitionJournals>()
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn bootstrap_verifies_snapshot_before_bal_replay() {
        let (factory, headers) = bootstrap_fixture();
        let filter = ConfiguredContractFilter::new([]);
        persist_bootstrap_tip(&factory, 3);
        let Some(PartialStateBootstrap::Sync(mut head)) =
            select_partial_state_bootstrap(&factory, &factory, &filter, true).unwrap()
        else {
            panic!("expected a persisted BAL-compatible pivot")
        };
        factory.begin_partial_state_sync(head, &filter).unwrap();
        factory.complete_partial_state_sync(head, &filter).unwrap();
        BalHistory::new(factory.bal_store().clone(), 64)
            .store_alloy(headers[4].num_hash(), &Vec::new())
            .unwrap();
        persist_bootstrap_tip(&factory, 4);
        assert_eq!(
            advance_partial_state_to_target(
                &NoRequestSnapClient,
                &factory,
                &factory,
                &filter,
                &mut head,
                headers[4].num_hash(),
                64,
            )
            .await
            .unwrap(),
            PartialStateAdvanceOutcome::Reconciled { advanced: 1, reverted: 0, pruned: 0 }
        );
        assert_eq!(head, bootstrap_pivot(&headers[4]));
        assert_eq!(factory.partial_state_checkpoint().unwrap().unwrap().pivot, head);
    }

    #[test]
    fn bootstrap_accepts_genesis_when_bal_is_active_from_genesis() {
        let (factory, headers) = bootstrap_fixture_at(0);
        let filter = ConfiguredContractFilter::new([]);
        assert_eq!(
            select_partial_state_bootstrap(&factory, &factory, &filter, true).unwrap(),
            Some(PartialStateBootstrap::Sync(bootstrap_pivot(&headers[0])))
        );
    }

    #[test]
    fn bootstrap_rejects_orphaned_persisted_pivot_and_mismatched_metadata() {
        let (factory, headers) = bootstrap_fixture();
        let filter = ConfiguredContractFilter::new([]);
        persist_bootstrap_tip(&factory, 3);
        let canonical: MockEthProvider = Default::default();
        let replacement = SealedHeader::seal_slow(Header {
            extra_data: Bytes::from_static(b"replacement"),
            ..headers[3].clone_header()
        });
        canonical.add_header(replacement.hash(), replacement.clone_header());
        assert_eq!(
            select_partial_state_bootstrap(&factory, &canonical, &filter, true).unwrap(),
            None
        );

        let pivot = bootstrap_pivot(&headers[3]);
        for wrong in [
            PartialStateSnapPivot { block_number: 2, ..pivot },
            PartialStateSnapPivot { state_root: B256::repeat_byte(1), ..pivot },
        ] {
            assert!(bal_compatible_pivot(&factory, factory.chain_spec().as_ref(), wrong)
                .unwrap_err()
                .to_string()
                .contains("does not match its header"));
        }
    }

    #[tokio::test]
    async fn bootstrap_missing_bal_after_activation_remains_fatal() {
        let (factory, headers) = bootstrap_fixture_at(36);
        let filter = ConfiguredContractFilter::new([]);
        let mut head = bootstrap_pivot(&headers[0]);
        factory.begin_partial_state_sync(head, &filter).unwrap();
        let checkpoint = factory.complete_partial_state_sync(head, &filter).unwrap();
        assert!(select_partial_state_bootstrap(&factory, &factory, &filter, true)
            .unwrap_err()
            .to_string()
            .contains("has no block access list commitment"));
        persist_bootstrap_tip(&factory, 1);
        for resume in [true, false] {
            assert!(select_partial_state_bootstrap(&factory, &factory, &filter, resume)
                .unwrap_err()
                .to_string()
                .contains("has no block access list commitment"));
        }
        assert!(advance_partial_state_to_target(
            &NoRequestSnapClient,
            &factory,
            &factory,
            &filter,
            &mut head,
            headers[1].num_hash(),
            64,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("has no block access list commitment"));
        let notification = CanonStateNotification::Commit {
            new: Arc::new(Chain::new(
                [recovered_empty_block(headers[1].clone_header(), headers[1].hash())],
                ExecutionOutcome::default(),
                BTreeMap::new(),
            )),
        };
        assert!(advance_partial_state_with_notification(
            &NoRequestSnapClient,
            &factory,
            &filter,
            &mut head,
            notification,
            64,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("has no block access list commitment"));
        assert_eq!(head, checkpoint.pivot);
        assert_eq!(factory.partial_state_checkpoint().unwrap(), Some(checkpoint));
    }

    #[derive(Debug)]
    struct CommitmentClient {
        account_hash: B256,
        responses: Mutex<VecDeque<(B256, PeerRequestResult<SnapResponse>)>>,
    }

    impl DownloadClient for CommitmentClient {
        fn report_bad_message(&self, _: PeerId) {}
        fn num_connected_peers(&self) -> usize {
            1
        }
    }

    impl SnapClient for CommitmentClient {
        type Output = NoRequestOutput;

        fn get_account_range_with_priority(
            &self,
            request: GetAccountRangeMessage,
            priority: Priority,
        ) -> Self::Output {
            assert_eq!(request.starting_hash, self.account_hash);
            assert_eq!(request.limit_hash, self.account_hash);
            assert_eq!(priority, Priority::High);
            let (root, response) =
                self.responses.lock().unwrap().pop_front().expect("unexpected account request");
            assert_eq!(request.root_hash, root);
            future::ready(response)
        }

        fn get_storage_ranges(&self, _: GetStorageRangesMessage) -> Self::Output {
            panic!("must not fetch untracked storage")
        }
        fn get_storage_ranges_with_priority(
            &self,
            _: GetStorageRangesMessage,
            _: Priority,
        ) -> Self::Output {
            panic!("must not fetch untracked storage")
        }
        fn get_byte_codes(&self, _: GetByteCodesMessage) -> Self::Output {
            panic!("must not fetch untracked code")
        }
        fn get_byte_codes_with_priority(
            &self,
            _: GetByteCodesMessage,
            _: Priority,
        ) -> Self::Output {
            panic!("must not fetch untracked code")
        }
        fn get_trie_nodes(&self, _: GetTrieNodesMessage) -> Self::Output {
            panic!("must not fetch storage trie nodes")
        }
        fn get_trie_nodes_with_priority(
            &self,
            _: GetTrieNodesMessage,
            _: Priority,
        ) -> Self::Output {
            panic!("must not fetch storage trie nodes")
        }
    }

    struct RecoveryFixture {
        factory: ProviderFactory<MockNodeTypesWithDB>,
        filter: ConfiguredContractFilter,
        pivot: PartialStateSnapPivot,
    }

    impl RecoveryFixture {
        const ADDRESS: Address = Address::repeat_byte(2);

        fn new() -> Self {
            let factory = create_test_provider_factory();
            let filter = ConfiguredContractFilter::new([]);
            let pivot = PartialStateSnapPivot {
                block_number: 0,
                block_hash: Header { state_root: Self::root(10), ..Default::default() }.hash_slow(),
                state_root: Self::root(10),
            };
            factory.begin_partial_state_sync(pivot, &filter).unwrap();
            let provider = factory.database_provider_rw().unwrap();
            provider
                .partial_state_snap_writer()
                .write_account(keccak256(Self::ADDRESS), Self::account(10))
                .unwrap();
            provider.tx_ref().put::<tables::HeaderNumbers>(pivot.block_hash, 0).unwrap();
            provider.commit().unwrap();
            factory.complete_partial_state_sync(pivot, &filter).unwrap();
            let static_files = factory.static_file_provider();
            let mut writer = static_files.latest_writer(StaticFileSegment::Headers).unwrap();
            writer
                .append_header(
                    &Header { state_root: pivot.state_root, ..Default::default() },
                    &pivot.block_hash,
                )
                .unwrap();
            writer.commit().unwrap();
            drop(writer);
            Self { factory, filter, pivot }
        }

        fn account(value: u64) -> TrieAccount {
            TrieAccount {
                balance: U256::from(100),
                storage_root: storage_root([(keccak256([0u8; 32]), U256::from(value))]),
                ..Default::default()
            }
        }

        fn root(value: u64) -> B256 {
            state_root_unhashed([(Self::ADDRESS, Self::account(value))])
        }

        fn block(&self, number: u64, parent_hash: B256, value: u64) -> RecoveredBlock<Block> {
            let bal = vec![AccountChanges::new(Self::ADDRESS).with_storage_change(
                SlotChanges::new(U256::ZERO, vec![StorageChange::new(1, U256::from(value))]),
            )];
            let header = Header {
                number,
                parent_hash,
                state_root: Self::root(value),
                block_access_list_hash: Some(compute_block_access_list_hash(&bal)),
                ..Default::default()
            };
            let hash = header.hash_slow();
            BalHistory::new(self.factory.bal_store().clone(), 64)
                .store_alloy(BlockNumHash::new(number, hash), &bal)
                .unwrap();
            recovered_empty_block(header, hash)
        }

        fn set_canonical(&self, blocks: &[RecoveredBlock<Block>]) {
            let height = self.factory.best_block_number().unwrap();
            let static_files = self.factory.static_file_provider();
            let mut writer = static_files.latest_writer(StaticFileSegment::Headers).unwrap();
            if height > 0 {
                writer.prune_headers(height).unwrap();
                writer.commit().unwrap();
                drop(writer);
                writer = static_files.latest_writer(StaticFileSegment::Headers).unwrap();
            }
            for block in blocks {
                writer.append_header(block.header(), &block.hash()).unwrap();
            }
            writer.commit().unwrap();
            drop(writer);
            let provider = self.factory.database_provider_rw().unwrap();
            provider
                .save_stage_checkpoint(
                    StageId::Finish,
                    StageCheckpoint::new(blocks.last().map_or(0, |block| block.number())),
                )
                .unwrap();
            provider.commit().unwrap();
        }

        fn response(value: u64) -> PeerRequestResult<SnapResponse> {
            let hash = keccak256(Self::ADDRESS);
            let path = Nibbles::unpack(hash);
            let body = alloy_rlp::encode(Self::account(value));
            let mut builder =
                HashBuilder::default().with_proof_retainer(ProofRetainer::from_iter([path]));
            builder.add_leaf(path, &body);
            assert_eq!(builder.root(), Self::root(value));
            let proof = builder
                .take_proof_nodes()
                .matching_nodes_sorted(&path)
                .into_iter()
                .map(|(_, node)| node)
                .collect();
            Ok(WithPeerId::new(
                PeerId::repeat_byte(1),
                SnapResponse::AccountRange(AccountRangeMessage {
                    request_id: 0,
                    accounts: vec![AccountData { hash, body: body.into() }],
                    proof,
                }),
            ))
        }

        fn unavailable() -> PeerRequestResult<SnapResponse> {
            Ok(WithPeerId::new(
                PeerId::repeat_byte(1),
                SnapResponse::AccountRange(AccountRangeMessage {
                    request_id: 0,
                    accounts: Vec::new(),
                    proof: Vec::new(),
                }),
            ))
        }

        fn client(responses: Vec<(B256, PeerRequestResult<SnapResponse>)>) -> CommitmentClient {
            CommitmentClient {
                account_hash: keccak256(Self::ADDRESS),
                responses: Mutex::new(responses.into()),
            }
        }

        fn assert_checkpoint(&self, head: PartialStateSnapPivot) {
            assert_eq!(self.factory.partial_state_checkpoint().unwrap().unwrap().pivot, head);
            assert_eq!(self.factory.partial_state_root(&self.filter).unwrap(), head.state_root);
            let provider = self.factory.database_provider_ro().unwrap();
            assert_eq!(provider.tx_ref().entries::<tables::PlainAccountState>().unwrap(), 0);
            assert_eq!(provider.tx_ref().entries::<tables::PlainStorageState>().unwrap(), 0);
            assert_eq!(provider.tx_ref().entries::<tables::HashedAccounts>().unwrap(), 0);
            assert_eq!(provider.tx_ref().entries::<tables::HashedStorages>().unwrap(), 0);
            assert_eq!(provider.tx_ref().entries::<tables::PartialStateStorages>().unwrap(), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn discards_commitment_if_canonical_target_changes_during_request() {
        let fixture = RecoveryFixture::new();
        let mut head = fixture.pivot;
        let old = fixture.block(1, head.block_hash, 11);
        let new = fixture.block(1, head.block_hash, 21);
        fixture.set_canonical(std::slice::from_ref(&old));
        let client = RecoveryFixture::client(vec![
            (old.state_root(), RecoveryFixture::unavailable()),
            (old.state_root(), RecoveryFixture::response(11)),
            (new.state_root(), RecoveryFixture::response(21)),
        ]);
        let (result, ()) = tokio::join!(
            advance_partial_state_to_target(
                &client,
                &fixture.factory,
                &fixture.factory,
                &fixture.filter,
                &mut head,
                old.num_hash(),
                64
            ),
            async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                fixture.set_canonical(std::slice::from_ref(&new));
            }
        );
        assert_eq!(
            result.unwrap(),
            PartialStateAdvanceOutcome::Reconciled { advanced: 1, reverted: 0, pruned: 0 }
        );
        assert_eq!(head.block_hash, new.hash());
        fixture.assert_checkpoint(head);
        assert!(client.responses.lock().unwrap().is_empty());
    }

    #[test]
    fn recovery_only_retries_commitment_availability_errors() {
        for error in [
            RequestError::Timeout,
            RequestError::ConnectionDropped,
            RequestError::UnsupportedCapability,
        ] {
            let error = eyre::Report::new(PartialStateAccountResolverError::Request(error))
                .wrap_err("resolving child commitment");
            assert!(commitment_is_unavailable(&error));
        }
        for error in [RequestError::BadResponse, RequestError::ChannelClosed] {
            assert!(!commitment_is_unavailable(&eyre::Report::new(
                PartialStateAccountResolverError::Request(error)
            )));
        }
        for error in [
            PartialStateAccountResolverError::UnexpectedResponse { account_hash: B256::ZERO },
            PartialStateAccountResolverError::InvalidAccountRange {
                account_hash: B256::ZERO,
                returned: 2,
            },
            PartialStateAccountResolverError::AccountDecode {
                account_hash: B256::ZERO,
                source: alloy_rlp::Error::UnexpectedLength,
            },
        ] {
            assert!(!commitment_is_unavailable(&eyre::Report::new(error)));
        }
        for error in [
            PartialStateTransitionError::ParentRootMismatch {
                expected: B256::ZERO,
                computed: B256::repeat_byte(1),
            },
            PartialStateTransitionError::ChildRootMismatch {
                expected: B256::ZERO,
                computed: B256::repeat_byte(1),
            },
            PartialStateTransitionError::BalHashMismatch {
                expected: B256::ZERO,
                computed: B256::repeat_byte(1),
            },
        ] {
            assert!(!commitment_is_unavailable(&eyre::Report::new(ProviderError::from(error))));
        }
        assert!(!commitment_is_unavailable(&eyre::eyre!("database failure")));
    }

    #[tokio::test]
    async fn advances_verified_pivot_to_canonical_child() {
        let factory = create_test_provider_factory();
        let tracked = address!("0000000000000000000000000000000000000001");
        let account_hash = keccak256(tracked);
        let slot = U256::from(1);
        let slot_hash = keccak256(slot.to_be_bytes::<32>());
        let parent_value = U256::from(10);
        let child_value = U256::from(11);
        let parent_account = TrieAccount {
            balance: U256::from(100),
            storage_root: storage_root([(slot_hash, parent_value)]),
            code_hash: KECCAK_EMPTY,
            ..Default::default()
        };
        let child_account = TrieAccount {
            balance: U256::from(101),
            storage_root: storage_root([(slot_hash, child_value)]),
            code_hash: KECCAK_EMPTY,
            ..Default::default()
        };
        let parent_root = state_root_unsorted([(account_hash, parent_account)]);
        let child_root = state_root_unsorted([(account_hash, child_account)]);
        let access_list = vec![AccountChanges::new(tracked)
            .with_storage_change(SlotChanges::new(slot, vec![StorageChange::new(1, child_value)]))
            .with_balance_change(BalanceChange::new(1, child_account.balance))];
        let bal_hash = compute_block_access_list_hash(&access_list);
        let parent_hash = B256::repeat_byte(0x42);
        let child_hash = B256::repeat_byte(0x43);

        let provider = factory.database_provider_rw().unwrap();
        {
            let mut writer = provider.partial_state_snap_writer();
            writer.write_account(account_hash, parent_account).unwrap();
            writer.write_storage(account_hash, slot_hash, parent_value).unwrap();
        }
        provider.commit().unwrap();

        let parent_header = Header { number: 0, state_root: parent_root, ..Default::default() };
        let child_header = Header {
            parent_hash,
            number: 1,
            state_root: child_root,
            block_access_list_hash: Some(bal_hash),
            ..Default::default()
        };
        let static_files = factory.static_file_provider();
        let mut writer = static_files.latest_writer(StaticFileSegment::Headers).unwrap();
        writer.append_header(&parent_header, &parent_hash).unwrap();
        writer.append_header(&child_header, &child_hash).unwrap();
        writer.commit().unwrap();
        drop(writer);
        let provider = factory.database_provider_rw().unwrap();
        provider.save_stage_checkpoint(StageId::Finish, StageCheckpoint::new(1)).unwrap();
        provider.commit().unwrap();

        let canonical_provider = BlockchainProvider::new(factory.clone()).unwrap();

        BalHistory::new(factory.bal_store().clone(), DEFAULT_PARTIAL_STATE_BAL_RETENTION)
            .store_alloy(BlockNumHash::new(1, child_hash), &access_list)
            .unwrap();

        let filter = ConfiguredContractFilter::new([tracked]);
        let mut head = PartialStateSnapPivot {
            block_number: 0,
            block_hash: parent_hash,
            state_root: parent_root,
        };
        let advanced = advance_partial_state_to_target(
            &NoRequestSnapClient,
            &factory,
            &canonical_provider,
            &filter,
            &mut head,
            BlockNumHash::new(1, child_hash),
            DEFAULT_PARTIAL_STATE_BAL_RETENTION,
        )
        .await
        .unwrap();

        assert_eq!(
            advanced,
            PartialStateAdvanceOutcome::Reconciled { advanced: 1, reverted: 0, pruned: 0 }
        );
        assert_eq!(head.block_number, 1);
        assert_eq!(head.block_hash, child_hash);
        assert_eq!(head.state_root, child_root);
        assert_eq!(factory.partial_state_root(&filter).unwrap(), child_root);
    }

    #[tokio::test]
    async fn consumes_reorg_notification_before_provider_canonical_state_changes() {
        let factory = create_test_provider_factory();
        let tracked = address!("0000000000000000000000000000000000000001");
        let account_hash = keccak256(tracked);
        let slot = U256::from(1);
        let slot_hash = keccak256(slot.to_be_bytes::<32>());
        let account = |balance, value| TrieAccount {
            balance: U256::from(balance),
            storage_root: storage_root([(slot_hash, U256::from(value))]),
            code_hash: KECCAK_EMPTY,
            ..Default::default()
        };
        let access_list = |balance, value| {
            vec![AccountChanges::new(tracked)
                .with_storage_change(SlotChanges::new(
                    slot,
                    vec![StorageChange::new(1, U256::from(value))],
                ))
                .with_balance_change(BalanceChange::new(1, U256::from(balance)))]
        };

        let genesis_account = account(100, 10);
        let old_account = account(101, 11);
        let new_account = account(201, 21);
        let genesis_root = state_root_unsorted([(account_hash, genesis_account)]);
        let old_root = state_root_unsorted([(account_hash, old_account)]);
        let new_root = state_root_unsorted([(account_hash, new_account)]);
        let old_bal = access_list(101, 11);
        let new_bal = access_list(201, 21);
        let genesis_hash = B256::repeat_byte(0x10);
        let old_hash = B256::repeat_byte(0xa1);
        let new_hash = B256::repeat_byte(0xb1);
        let genesis_header = Header { number: 0, state_root: genesis_root, ..Default::default() };
        let old_header = Header {
            parent_hash: genesis_hash,
            number: 1,
            state_root: old_root,
            block_access_list_hash: Some(compute_block_access_list_hash(&old_bal)),
            ..Default::default()
        };
        let new_header = Header {
            parent_hash: genesis_hash,
            number: 1,
            state_root: new_root,
            block_access_list_hash: Some(compute_block_access_list_hash(&new_bal)),
            ..Default::default()
        };

        let provider = factory.database_provider_rw().unwrap();
        {
            let mut writer = provider.partial_state_snap_writer();
            writer.write_account(account_hash, genesis_account).unwrap();
            writer.write_storage(account_hash, slot_hash, U256::from(10)).unwrap();
        }
        provider.commit().unwrap();

        let static_files = factory.static_file_provider();
        let mut writer = static_files.latest_writer(StaticFileSegment::Headers).unwrap();
        writer.append_header(&genesis_header, &genesis_hash).unwrap();
        writer.append_header(&old_header, &old_hash).unwrap();
        writer.commit().unwrap();
        drop(writer);
        let provider = factory.database_provider_rw().unwrap();
        provider.save_stage_checkpoint(StageId::Finish, StageCheckpoint::new(1)).unwrap();
        provider.commit().unwrap();

        let history =
            BalHistory::new(factory.bal_store().clone(), DEFAULT_PARTIAL_STATE_BAL_RETENTION);
        history.store_alloy(BlockNumHash::new(1, old_hash), &old_bal).unwrap();
        history.store_alloy(BlockNumHash::new(1, new_hash), &new_bal).unwrap();

        let canonical_provider = BlockchainProvider::new(factory.clone()).unwrap();
        let filter = ConfiguredContractFilter::new([tracked]);
        let mut head = PartialStateSnapPivot {
            block_number: 0,
            block_hash: genesis_hash,
            state_root: genesis_root,
        };
        advance_partial_state_to_target(
            &NoRequestSnapClient,
            &factory,
            &canonical_provider,
            &filter,
            &mut head,
            BlockNumHash::new(1, old_hash),
            DEFAULT_PARTIAL_STATE_BAL_RETENTION,
        )
        .await
        .unwrap();

        let notification = CanonStateNotification::Reorg {
            old: Arc::new(Chain::new(
                [recovered_empty_block(old_header, old_hash)],
                ExecutionOutcome::default(),
                BTreeMap::new(),
            )),
            new: Arc::new(Chain::new(
                [recovered_empty_block(new_header, new_hash)],
                ExecutionOutcome::default(),
                BTreeMap::new(),
            )),
        };
        let outcome = advance_partial_state_with_notification(
            &NoRequestSnapClient,
            &factory,
            &filter,
            &mut head,
            notification,
            DEFAULT_PARTIAL_STATE_BAL_RETENTION,
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            PartialStateAdvanceOutcome::Reconciled { advanced: 1, reverted: 1, pruned: 0 }
        );
        assert_eq!(
            head,
            PartialStateSnapPivot { block_number: 1, block_hash: new_hash, state_root: new_root }
        );
        assert_eq!(canonical_provider.sealed_header(1).unwrap().unwrap().hash(), old_hash);
        assert_eq!(factory.partial_state_root(&filter).unwrap(), new_root);

        let revert = CanonStateNotification::Reorg {
            old: Arc::new(Chain::new(
                [recovered_empty_block(
                    Header {
                        parent_hash: genesis_hash,
                        number: 1,
                        state_root: new_root,
                        block_access_list_hash: Some(compute_block_access_list_hash(&new_bal)),
                        ..Default::default()
                    },
                    new_hash,
                )],
                ExecutionOutcome::default(),
                BTreeMap::new(),
            )),
            new: Arc::new(Chain::default()),
        };
        let outcome = advance_partial_state_with_notification(
            &NoRequestSnapClient,
            &factory,
            &filter,
            &mut head,
            revert,
            DEFAULT_PARTIAL_STATE_BAL_RETENTION,
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            PartialStateAdvanceOutcome::Reconciled { advanced: 0, reverted: 1, pruned: 0 }
        );
        assert_eq!(
            head,
            PartialStateSnapPivot {
                block_number: 0,
                block_hash: genesis_hash,
                state_root: genesis_root,
            }
        );
        assert_eq!(factory.partial_state_root(&filter).unwrap(), genesis_root);
    }

    #[tokio::test]
    async fn replays_canonical_reorg_and_requests_resync_beyond_retention() {
        let factory = create_test_provider_factory();
        let tracked = address!("0000000000000000000000000000000000000001");
        let account_hash = keccak256(tracked);
        let slot = U256::from(1);
        let slot_hash = keccak256(slot.to_be_bytes::<32>());
        let account = |balance, value| TrieAccount {
            balance: U256::from(balance),
            storage_root: storage_root([(slot_hash, U256::from(value))]),
            code_hash: KECCAK_EMPTY,
            ..Default::default()
        };
        let access_list = |balance, value| {
            vec![AccountChanges::new(tracked)
                .with_storage_change(SlotChanges::new(
                    slot,
                    vec![StorageChange::new(1, U256::from(value))],
                ))
                .with_balance_change(BalanceChange::new(1, U256::from(balance)))]
        };

        let genesis_account = account(100, 10);
        let genesis_root = state_root_unsorted([(account_hash, genesis_account)]);
        let genesis_hash = B256::repeat_byte(0x10);

        let a1_account = account(101, 11);
        let a2_account = account(102, 12);
        let a1_root = state_root_unsorted([(account_hash, a1_account)]);
        let a2_root = state_root_unsorted([(account_hash, a2_account)]);
        let a1_bal = access_list(101, 11);
        let a2_bal = access_list(102, 12);
        let a1_hash = B256::repeat_byte(0xa1);
        let a2_hash = B256::repeat_byte(0xa2);

        let provider = factory.database_provider_rw().unwrap();
        {
            let mut writer = provider.partial_state_snap_writer();
            writer.write_account(account_hash, genesis_account).unwrap();
            writer.write_storage(account_hash, slot_hash, U256::from(10)).unwrap();
        }
        provider.commit().unwrap();

        let static_files = factory.static_file_provider();
        let mut writer = static_files.latest_writer(StaticFileSegment::Headers).unwrap();
        writer
            .append_header(
                &Header { number: 0, state_root: genesis_root, ..Default::default() },
                &genesis_hash,
            )
            .unwrap();
        writer
            .append_header(
                &Header {
                    parent_hash: genesis_hash,
                    number: 1,
                    state_root: a1_root,
                    block_access_list_hash: Some(compute_block_access_list_hash(&a1_bal)),
                    ..Default::default()
                },
                &a1_hash,
            )
            .unwrap();
        writer
            .append_header(
                &Header {
                    parent_hash: a1_hash,
                    number: 2,
                    state_root: a2_root,
                    block_access_list_hash: Some(compute_block_access_list_hash(&a2_bal)),
                    ..Default::default()
                },
                &a2_hash,
            )
            .unwrap();
        writer.commit().unwrap();
        drop(writer);
        let provider = factory.database_provider_rw().unwrap();
        provider.save_stage_checkpoint(StageId::Finish, StageCheckpoint::new(2)).unwrap();
        provider.commit().unwrap();

        let history =
            BalHistory::new(factory.bal_store().clone(), DEFAULT_PARTIAL_STATE_BAL_RETENTION);
        history.store_alloy(BlockNumHash::new(1, a1_hash), &a1_bal).unwrap();
        history.store_alloy(BlockNumHash::new(2, a2_hash), &a2_bal).unwrap();

        let canonical_provider = BlockchainProvider::new(factory.clone()).unwrap();
        let filter = ConfiguredContractFilter::new([tracked]);
        let mut head = PartialStateSnapPivot {
            block_number: 0,
            block_hash: genesis_hash,
            state_root: genesis_root,
        };
        let outcome = advance_partial_state_to_target(
            &NoRequestSnapClient,
            &factory,
            &canonical_provider,
            &filter,
            &mut head,
            BlockNumHash::new(2, a2_hash),
            DEFAULT_PARTIAL_STATE_BAL_RETENTION,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            PartialStateAdvanceOutcome::Reconciled { advanced: 2, reverted: 0, pruned: 0 }
        );

        let b1_account = account(201, 21);
        let b2_account = account(202, 22);
        let b1_root = state_root_unsorted([(account_hash, b1_account)]);
        let b2_root = state_root_unsorted([(account_hash, b2_account)]);
        let b1_bal = access_list(201, 21);
        let b2_bal = access_list(202, 22);
        let b1_hash = B256::repeat_byte(0xb1);
        let b2_hash = B256::repeat_byte(0xb2);

        let mut writer = static_files.latest_writer(StaticFileSegment::Headers).unwrap();
        writer.prune_headers(2).unwrap();
        writer.commit().unwrap();
        drop(writer);
        let mut writer = static_files.latest_writer(StaticFileSegment::Headers).unwrap();
        writer
            .append_header(
                &Header {
                    parent_hash: genesis_hash,
                    number: 1,
                    state_root: b1_root,
                    block_access_list_hash: Some(compute_block_access_list_hash(&b1_bal)),
                    ..Default::default()
                },
                &b1_hash,
            )
            .unwrap();
        writer
            .append_header(
                &Header {
                    parent_hash: b1_hash,
                    number: 2,
                    state_root: b2_root,
                    block_access_list_hash: Some(compute_block_access_list_hash(&b2_bal)),
                    ..Default::default()
                },
                &b2_hash,
            )
            .unwrap();
        writer.commit().unwrap();
        drop(writer);
        history.store_alloy(BlockNumHash::new(1, b1_hash), &b1_bal).unwrap();
        history.store_alloy(BlockNumHash::new(2, b2_hash), &b2_bal).unwrap();

        let outcome = advance_partial_state_to_target(
            &NoRequestSnapClient,
            &factory,
            &canonical_provider,
            &filter,
            &mut head,
            BlockNumHash::new(2, b2_hash),
            1,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            PartialStateAdvanceOutcome::Reconciled { advanced: 2, reverted: 2, pruned: 1 }
        );
        assert_eq!(
            head,
            PartialStateSnapPivot { block_number: 2, block_hash: b2_hash, state_root: b2_root }
        );
        assert_eq!(factory.partial_state_root(&filter).unwrap(), b2_root);

        let provider = factory.database_provider_ro().unwrap();
        assert_eq!(
            provider.tx_ref().get::<tables::PartialStateTransitionJournals>(1).unwrap(),
            None
        );
        assert_eq!(
            provider
                .tx_ref()
                .get::<tables::PartialStateTransitionJournals>(2)
                .unwrap()
                .unwrap()
                .block_hash,
            b2_hash
        );
        drop(provider);

        let c1_hash = B256::repeat_byte(0xc1);
        let c2_hash = B256::repeat_byte(0xc2);
        let mut writer = static_files.latest_writer(StaticFileSegment::Headers).unwrap();
        writer.prune_headers(2).unwrap();
        writer.commit().unwrap();
        drop(writer);
        let mut writer = static_files.latest_writer(StaticFileSegment::Headers).unwrap();
        writer
            .append_header(
                &Header {
                    parent_hash: genesis_hash,
                    number: 1,
                    state_root: B256::repeat_byte(0xc1),
                    block_access_list_hash: Some(B256::repeat_byte(0x31)),
                    ..Default::default()
                },
                &c1_hash,
            )
            .unwrap();
        writer
            .append_header(
                &Header {
                    parent_hash: c1_hash,
                    number: 2,
                    state_root: B256::repeat_byte(0xc2),
                    block_access_list_hash: Some(B256::repeat_byte(0x32)),
                    ..Default::default()
                },
                &c2_hash,
            )
            .unwrap();
        writer.commit().unwrap();
        drop(writer);

        let outcome = advance_partial_state_to_target(
            &NoRequestSnapClient,
            &factory,
            &canonical_provider,
            &filter,
            &mut head,
            BlockNumHash::new(2, c2_hash),
            1,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            PartialStateAdvanceOutcome::ResyncRequired {
                unavailable_block: BlockNumHash::new(1, b1_hash),
                reverted: 1,
            }
        );

        factory.reset_partial_state().unwrap();
        assert_eq!(factory.partial_state_root(&filter).unwrap(), EMPTY_ROOT_HASH);
        assert_eq!(
            factory
                .database_provider_ro()
                .unwrap()
                .tx_ref()
                .get::<tables::PartialStateTransitionJournals>(2)
                .unwrap(),
            None
        );
    }

    #[test]
    fn retains_payload_bal_by_block_hash() {
        let store = reth_provider::BalStoreHandle::new(reth_provider::InMemoryBalStore::default());
        let num_hash = BlockNumHash::new(1, B256::repeat_byte(0x44));
        let raw = Bytes::from_static(&[0xc0]);

        retain_payload_bal(&store, num_hash, &raw);

        assert_eq!(store.get_by_hash(num_hash.hash).unwrap(), Some(raw));
    }

    #[tokio::test]
    async fn advances_and_reorgs_with_remote_commitments_without_full_state() {
        let factory = create_test_provider_factory();
        let tracked = Address::repeat_byte(1);
        let untracked = Address::repeat_byte(2);
        let filter = ConfiguredContractFilter::new([tracked]);
        let slot = U256::from(1);
        let slot_hash = keccak256(slot.to_be_bytes::<32>());
        let untracked_hash = keccak256(untracked);
        let accounts = |value: u64| {
            [
                (
                    tracked,
                    TrieAccount {
                        balance: U256::from(100),
                        storage_root: storage_root([(slot_hash, U256::from(value))]),
                        ..Default::default()
                    },
                ),
                (
                    untracked,
                    TrieAccount {
                        balance: U256::from(200),
                        storage_root: storage_root([(slot_hash, U256::from(value + 10))]),
                        ..Default::default()
                    },
                ),
            ]
        };
        let bal = |value: u64| {
            vec![
                AccountChanges::new(tracked).with_storage_change(SlotChanges::new(
                    slot,
                    vec![StorageChange::new(1, U256::from(value))],
                )),
                AccountChanges::new(untracked).with_storage_change(SlotChanges::new(
                    slot,
                    vec![StorageChange::new(1, U256::from(value + 10))],
                )),
            ]
        };
        let response = |value: u64| {
            let path = Nibbles::unpack(untracked_hash);
            let mut builder =
                HashBuilder::default().with_proof_retainer(ProofRetainer::from_iter([path]));
            let mut leaves =
                accounts(value).map(|(address, account)| (keccak256(address), account));
            leaves.sort_unstable_by_key(|(hash, _)| *hash);
            for (hash, account) in leaves {
                builder.add_leaf(Nibbles::unpack(hash), &alloy_rlp::encode(account));
            }
            assert_eq!(builder.root(), state_root_unhashed(accounts(value)));
            let proof = builder
                .take_proof_nodes()
                .matching_nodes_sorted(&path)
                .into_iter()
                .map(|(_, node)| node)
                .collect();
            Ok(WithPeerId::new(
                PeerId::repeat_byte(1),
                SnapResponse::AccountRange(AccountRangeMessage {
                    request_id: 0,
                    accounts: vec![AccountData {
                        hash: untracked_hash,
                        body: alloy_rlp::encode(accounts(value)[1].1).into(),
                    }],
                    proof,
                }),
            ))
        };
        let genesis_hash = B256::repeat_byte(0x10);
        let mut head = PartialStateSnapPivot {
            block_number: 0,
            block_hash: genesis_hash,
            state_root: state_root_unhashed(accounts(10)),
        };
        factory.begin_partial_state_sync(head, &filter).unwrap();
        let provider = factory.database_provider_rw().unwrap();
        for (address, account) in accounts(10) {
            provider
                .partial_state_snap_writer()
                .write_account(keccak256(address), account)
                .unwrap();
        }
        provider
            .partial_state_snap_writer()
            .write_storage(keccak256(tracked), slot_hash, U256::from(10))
            .unwrap();
        provider.commit().unwrap();
        let checkpoint = factory.complete_partial_state_sync(head, &filter).unwrap();

        let make_header = |number, parent_hash, value| Header {
            number,
            parent_hash,
            state_root: state_root_unhashed(accounts(value)),
            block_access_list_hash: Some(compute_block_access_list_hash(&bal(value))),
            ..Default::default()
        };
        let first_hash = B256::repeat_byte(0x11);
        let second_hash = B256::repeat_byte(0x12);
        let replacement_hash = B256::repeat_byte(0x22);
        let first = make_header(1, genesis_hash, 11);
        let second = make_header(2, first_hash, 12);
        let replacement = make_header(2, first_hash, 22);
        let files = factory.static_file_provider();
        let mut writer = files.latest_writer(StaticFileSegment::Headers).unwrap();
        writer
            .append_header(
                &Header { state_root: head.state_root, ..Default::default() },
                &genesis_hash,
            )
            .unwrap();
        writer.append_header(&first, &first_hash).unwrap();
        writer.commit().unwrap();
        drop(writer);
        let provider = factory.database_provider_rw().unwrap();
        provider.save_stage_checkpoint(StageId::Finish, StageCheckpoint::new(1)).unwrap();
        provider.commit().unwrap();
        let history = BalHistory::new(factory.bal_store().clone(), 64);
        for (number, hash, value) in
            [(1, first_hash, 11), (2, second_hash, 12), (2, replacement_hash, 22)]
        {
            history.store_alloy(BlockNumHash::new(number, hash), &bal(value)).unwrap();
        }
        let client = CommitmentClient {
            account_hash: untracked_hash,
            responses: Mutex::new(VecDeque::from([
                (first.state_root, response(10)), // Correct account, wrong state version.
                (first.state_root, Err(RequestError::UnsupportedCapability)),
                (first.state_root, response(11)),
                (second.state_root, response(12)),
                (replacement.state_root, response(22)),
            ])),
        };
        for _ in 0..2 {
            assert!(advance_partial_state_to_target(
                &client,
                &factory,
                &factory,
                &filter,
                &mut head,
                BlockNumHash::new(1, first_hash),
                64
            )
            .await
            .is_err());
            assert_eq!(head, checkpoint.pivot);
            assert_eq!(factory.partial_state_checkpoint().unwrap(), Some(checkpoint));
            assert_eq!(factory.partial_state_root(&filter).unwrap(), checkpoint.pivot.state_root);
            assert_eq!(
                factory
                    .database_provider_ro()
                    .unwrap()
                    .tx_ref()
                    .entries::<tables::PartialStateTransitionJournals>()
                    .unwrap(),
                0
            );
        }
        advance_partial_state_to_target(
            &client,
            &factory,
            &factory,
            &filter,
            &mut head,
            BlockNumHash::new(1, first_hash),
            64,
        )
        .await
        .unwrap();
        let chain = |header: Header, hash| {
            Arc::new(Chain::new(
                [recovered_empty_block(header, hash)],
                ExecutionOutcome::default(),
                BTreeMap::new(),
            ))
        };
        let old = chain(second, second_hash);
        advance_partial_state_with_notification(
            &client,
            &factory,
            &filter,
            &mut head,
            CanonStateNotification::Commit { new: old.clone() },
            64,
        )
        .await
        .unwrap();
        assert_eq!(head.state_root, state_root_unhashed(accounts(12)));
        let result = advance_partial_state_with_notification(
            &client,
            &factory,
            &filter,
            &mut head,
            CanonStateNotification::Reorg { old, new: chain(replacement, replacement_hash) },
            64,
        )
        .await
        .unwrap();
        assert_eq!(
            result,
            PartialStateAdvanceOutcome::Reconciled { advanced: 1, reverted: 1, pruned: 0 }
        );
        assert_eq!(head.block_hash, replacement_hash);
        assert_eq!(head.state_root, state_root_unhashed(accounts(22)));
        assert_eq!(factory.partial_state_checkpoint().unwrap().unwrap().pivot, head);
        assert_eq!(factory.partial_state_root(&filter).unwrap(), head.state_root);
        assert!(client.responses.lock().unwrap().is_empty());
        let provider = factory.database_provider_ro().unwrap();
        assert_eq!(provider.tx_ref().entries::<tables::PlainAccountState>().unwrap(), 0);
        assert_eq!(provider.tx_ref().entries::<tables::PlainStorageState>().unwrap(), 0);
        assert_eq!(provider.tx_ref().entries::<tables::HashedAccounts>().unwrap(), 0);
        assert_eq!(provider.tx_ref().entries::<tables::HashedStorages>().unwrap(), 0);
        assert_eq!(provider.tx_ref().entries::<tables::PartialStateStorages>().unwrap(), 1);
        assert_eq!(
            provider.tx_ref().get::<tables::PartialStateStorageRoots>(untracked_hash).unwrap(),
            Some(accounts(22)[1].1.storage_root)
        );
    }
}
