use alloy_consensus::BlockHeader;
use alloy_eips::{BlockNumHash, NumHash};
use alloy_primitives::{keccak256, Address, Bytes, Sealed};
use reth_chain_state::CanonStateNotification;
use reth_downloaders::snap::resolve_partial_state_accounts;
use reth_network_p2p::snap::client::SnapClient;
use reth_primitives_traits::NodePrimitives;
use reth_provider::{
    providers::{BlockchainProvider, ProviderNodeTypes},
    BlockNumReader, HeaderProvider, ProviderFactory,
};
use reth_storage_api::{
    errors::provider::{PartialStateTransitionError, ProviderError, ProviderResult},
    BalProvider, BalStoreHandle, ConfiguredContractFilter, ContractFilter,
    PartialStateResolvedAccounts, PartialStateSnapPivot, PartialStateTransition,
    PartialStateTransitionProvider, StateProvider, StateProviderFactory,
};
use reth_tracing::tracing::{debug, info, warn};
use reth_trie_common::{KeccakKeyHasher, TrieAccount};

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

/// Result of reconciling partial state with the persisted canonical chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PartialStateAdvanceOutcome {
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

/// Reconciles verified partial state with the persisted canonical chain using retained BALs.
pub(crate) async fn advance_partial_state_to_target<N, Client>(
    client: &Client,
    provider_factory: &ProviderFactory<N>,
    canonical_provider: &BlockchainProvider<N>,
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
        let canonical_tip_number = canonical_provider.best_block_number()?;
        let canonical_tip = canonical_provider
            .sealed_header(canonical_tip_number)?
            .ok_or_else(|| eyre::eyre!("canonical header {canonical_tip_number} is unavailable"))?
            .num_hash();
        if canonical_tip != notified_target && observed_canonical_tip != Some(canonical_tip) {
            debug!(
                target: "reth::cli",
                notified_block = notified_target.number,
                notified_hash = %notified_target.hash,
                canonical_block = canonical_tip.number,
                canonical_hash = %canonical_tip.hash,
                "Reconciling partial state to current persisted canonical tip"
            );
        }
        observed_canonical_tip = Some(canonical_tip);

        let canonical_head = canonical_provider.sealed_header(head.block_number)?;
        let head_is_canonical =
            canonical_head.as_ref().is_some_and(|header| header.hash() == head.block_hash) &&
                head.block_number <= canonical_tip.number;
        if !head_is_canonical {
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
            .sealed_header(number)?
            .ok_or_else(|| eyre::eyre!("canonical header {number} is unavailable"))?;
        if header.parent_hash() != head.block_hash {
            continue
        }

        let block_hash = header.hash();
        let expected_bal_hash = header.block_access_list_hash().ok_or_else(|| {
            eyre::eyre!(
                "canonical block {number} ({block_hash}) has no block access list commitment"
            )
        })?;
        let decoded_bal =
            provider_factory.bal_store().get_decoded_by_hash(block_hash)?.ok_or_else(|| {
                eyre::eyre!(
                    "block access list for canonical block {number} ({block_hash}) is unavailable"
                )
            })?;
        decoded_bal.ensure_hash(expected_bal_hash)?;

        let needs_resolution = decoded_bal.as_bal().iter().any(|changes| {
            !changes.storage_changes.is_empty() && !filter.should_sync_storage(&changes.address)
        });
        let resolved_accounts = if needs_resolution {
            match canonical_provider.state_by_block_hash(block_hash) {
                Ok(state) => resolve_partial_state_accounts_from_state(
                    state.as_ref(),
                    decoded_bal.as_bal(),
                    filter,
                )?,
                Err(err) => {
                    warn!(
                        target: "reth::cli",
                        block_number = number,
                        %block_hash,
                        %err,
                        "Canonical child state unavailable locally; falling back to snap account resolution"
                    );
                    resolve_partial_state_accounts(
                        client,
                        header.state_root(),
                        decoded_bal.as_bal(),
                        filter,
                    )
                    .await?
                }
            }
        } else {
            PartialStateResolvedAccounts::new()
        };
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
/// ascending order. The notification owns the executed replacement branch, so account
/// commitments can still be resolved if that branch is superseded before this task runs.
pub(crate) async fn advance_partial_state_with_notification<N, Client>(
    client: &Client,
    provider_factory: &ProviderFactory<N>,
    canonical_provider: &BlockchainProvider<N>,
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
        let fork = committed_chain.fork_block();
        let fork_state = match canonical_provider.state_by_block_hash(fork.hash) {
            Ok(state) => Some(state),
            Err(err) => {
                warn!(
                    target: "reth::cli",
                    block_number = fork.number,
                    block_hash = %fork.hash,
                    %err,
                    "Canonical notification fork state unavailable; falling back to snap account resolution"
                );
                None
            }
        };

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

            let expected_bal_hash = block.block_access_list_hash().ok_or_else(|| {
                eyre::eyre!(
                    "canonical block {number} ({block_hash}) has no block access list commitment"
                )
            })?;
            let decoded_bal = provider_factory
                .bal_store()
                .get_decoded_by_hash(block_hash)?
                .ok_or_else(|| {
                    eyre::eyre!(
                        "block access list for canonical block {number} ({block_hash}) is unavailable"
                    )
                })?;
            decoded_bal.ensure_hash(expected_bal_hash)?;

            let needs_resolution = decoded_bal.as_bal().iter().any(|changes| {
                !changes.storage_changes.is_empty() && !filter.should_sync_storage(&changes.address)
            });
            let resolved_accounts = if needs_resolution {
                if let Some(fork_state) = fork_state.as_deref() {
                    let outcome =
                        committed_chain.execution_outcome_at_block(number).ok_or_else(|| {
                            eyre::eyre!("execution outcome for canonical block {number} is missing")
                        })?;
                    let hashed_state = outcome.hash_state_slow::<KeccakKeyHasher>();
                    resolve_partial_state_accounts_with(decoded_bal.as_bal(), filter, |address| {
                        let account = match outcome.account(&address) {
                            Some(account) => account,
                            None => fork_state.basic_account(&address)?,
                        };
                        let Some(account) = account else { return Ok(None) };
                        let storage = hashed_state
                            .storages
                            .get(&keccak256(address))
                            .cloned()
                            .unwrap_or_default();
                        let storage_root = fork_state.storage_root(address, storage)?;
                        Ok(Some(account.into_trie_account(storage_root)))
                    })?
                } else {
                    resolve_partial_state_accounts(
                        client,
                        block.state_root(),
                        decoded_bal.as_bal(),
                        filter,
                    )
                    .await?
                }
            } else {
                PartialStateResolvedAccounts::new()
            };
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

fn resolve_partial_state_accounts_from_state(
    state: &dyn StateProvider,
    access_list: &[alloy_eips::eip7928::AccountChanges],
    filter: &dyn ContractFilter,
) -> ProviderResult<PartialStateResolvedAccounts> {
    resolve_partial_state_accounts_with(access_list, filter, |address| {
        let Some(account) = state.basic_account(&address)? else { return Ok(None) };
        let storage_root = state.storage_root(address, Default::default())?;
        Ok(Some(account.into_trie_account(storage_root)))
    })
}

fn resolve_partial_state_accounts_with(
    access_list: &[alloy_eips::eip7928::AccountChanges],
    filter: &dyn ContractFilter,
    mut resolve: impl FnMut(Address) -> ProviderResult<Option<TrieAccount>>,
) -> ProviderResult<PartialStateResolvedAccounts> {
    let mut resolved = PartialStateResolvedAccounts::new();
    for changes in access_list {
        if !changes.storage_changes.is_empty() &&
            !filter.should_sync_storage(&changes.address) &&
            !resolved.contains_key(&changes.address)
        {
            resolved.insert(changes.address, resolve(changes.address)?);
        }
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{
        constants::{EMPTY_ROOT_HASH, KECCAK_EMPTY},
        Header,
    };
    use alloy_eips::eip7928::{
        compute_block_access_list_hash, AccountChanges, BalanceChange, SlotChanges, StorageChange,
    };
    use alloy_primitives::{address, B256, U256};
    use futures::future;
    use reth_db_api::{tables, transaction::DbTx};
    use reth_eth_wire_types::snap::{
        GetAccountRangeMessage, GetByteCodesMessage, GetStorageRangesMessage, GetTrieNodesMessage,
    };
    use reth_ethereum_primitives::{Block, BlockBody};
    use reth_execution_types::{Chain, ExecutionOutcome};
    use reth_network_p2p::{
        download::DownloadClient,
        error::{PeerRequestResult, RequestError},
        priority::Priority,
        snap::client::SnapResponse,
    };
    use reth_network_peers::PeerId;
    use reth_primitives_traits::{RecoveredBlock, SealedBlock, SealedHeader};
    use reth_provider::{
        test_utils::create_test_provider_factory, StaticFileProviderFactory, StaticFileSegment,
        StaticFileWriter,
    };
    use reth_stages::{StageCheckpoint, StageId};
    use reth_storage_api::{
        BalHistory, DBProvider, DatabaseProviderFactory, PartialStateRootProvider,
        PartialStateSnapWriter, StageCheckpointWriter, DEFAULT_PARTIAL_STATE_BAL_RETENTION,
    };
    use reth_trie::root::{state_root_unsorted, storage_root};
    use reth_trie_common::TrieAccount;
    use std::{collections::BTreeMap, sync::Arc};

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
            &canonical_provider,
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
            &canonical_provider,
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

    #[test]
    fn resolves_only_untracked_storage_commitments_from_canonical_state() {
        let tracked = address!("0000000000000000000000000000000000000001");
        let untracked = address!("0000000000000000000000000000000000000002");
        let account_only = address!("0000000000000000000000000000000000000003");
        let resolved_account = TrieAccount {
            nonce: 1,
            balance: U256::from(2),
            storage_root: B256::repeat_byte(0x44),
            code_hash: KECCAK_EMPTY,
        };
        let access_list = vec![
            AccountChanges::new(tracked).with_storage_change(SlotChanges::new(
                U256::from(1),
                vec![StorageChange::new(1, U256::from(2))],
            )),
            AccountChanges::new(untracked).with_storage_change(SlotChanges::new(
                U256::from(3),
                vec![StorageChange::new(1, U256::from(4))],
            )),
            AccountChanges::new(account_only)
                .with_balance_change(BalanceChange::new(1, U256::from(5))),
        ];
        let filter = ConfiguredContractFilter::new([tracked]);
        let mut requested = Vec::new();

        let resolved = resolve_partial_state_accounts_with(&access_list, &filter, |address| {
            requested.push(address);
            Ok(Some(resolved_account))
        })
        .unwrap();

        assert_eq!(requested, vec![untracked]);
        assert_eq!(resolved.get(&untracked), Some(&Some(resolved_account)));
    }
}
