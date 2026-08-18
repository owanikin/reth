use alloy_consensus::BlockHeader;
use alloy_eips::{BlockNumHash, NumHash};
use alloy_primitives::{keccak256, Address, Bytes, Sealed};
use reth_downloaders::snap::resolve_partial_state_accounts;
use reth_network_p2p::snap::client::SnapClient;
use reth_provider::{
    providers::{BlockchainProvider, ProviderNodeTypes},
    HeaderProvider, ProviderFactory,
};
use reth_storage_api::{
    errors::provider::ProviderResult, BalProvider, BalStoreHandle, ConfiguredContractFilter,
    ContractFilter, PartialStateResolvedAccounts, PartialStateSnapPivot, PartialStateTransition,
    PartialStateTransitionProvider, StateProvider, StateProviderFactory,
};
use reth_tracing::tracing::{info, warn};
use reth_trie_common::TrieAccount;

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

/// Advances verified partial state through selected canonical blocks using their BALs.
pub(crate) async fn advance_partial_state_to_target<N, Client>(
    client: &Client,
    provider_factory: &ProviderFactory<N>,
    canonical_provider: &BlockchainProvider<N>,
    filter: &ConfiguredContractFilter,
    head: &mut PartialStateSnapPivot,
    target: BlockNumHash,
) -> eyre::Result<u64>
where
    N: ProviderNodeTypes + 'static,
    Client: SnapClient,
{
    if target.number < head.block_number {
        eyre::bail!(
            "partial-state canonical target moved behind the current head: current={} target={}",
            head.block_number,
            target.number
        )
    }
    if target.number == head.block_number {
        eyre::ensure!(
            target.hash == head.block_hash,
            "partial-state canonical target changed at block {}: current={} target={}",
            target.number,
            head.block_hash,
            target.hash
        );
        return Ok(0)
    }

    let mut advanced = 0;
    for number in (head.block_number + 1)..=target.number {
        let header = canonical_provider
            .sealed_header(number)?
            .ok_or_else(|| eyre::eyre!("canonical header {number} is unavailable"))?;
        if number == target.number {
            eyre::ensure!(
                header.hash() == target.hash,
                "canonical target hash changed at block {number}: event={} current={}",
                target.hash,
                header.hash()
            );
        }
        eyre::ensure!(
            header.parent_hash() == head.block_hash,
            "partial-state canonical chain is not contiguous at block {number}: expected parent {}, got {}",
            head.block_hash,
            header.parent_hash()
        );

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
    Ok(advanced)
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
    use alloy_consensus::{constants::KECCAK_EMPTY, Header};
    use alloy_eips::eip7928::{
        compute_block_access_list_hash, AccountChanges, BalanceChange, SlotChanges, StorageChange,
    };
    use alloy_primitives::{address, B256, U256};
    use futures::future;
    use reth_eth_wire_types::snap::{
        GetAccountRangeMessage, GetByteCodesMessage, GetStorageRangesMessage, GetTrieNodesMessage,
    };
    use reth_network_p2p::{
        download::DownloadClient,
        error::{PeerRequestResult, RequestError},
        priority::Priority,
        snap::client::SnapResponse,
    };
    use reth_network_peers::PeerId;
    use reth_provider::{
        test_utils::create_test_provider_factory, StaticFileProviderFactory, StaticFileSegment,
        StaticFileWriter,
    };
    use reth_storage_api::{
        BalHistory, DBProvider, DatabaseProviderFactory, PartialStateRootProvider,
        PartialStateSnapWriter, DEFAULT_PARTIAL_STATE_BAL_RETENTION,
    };
    use reth_trie::root::{state_root_unsorted, storage_root};
    use reth_trie_common::TrieAccount;

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
        )
        .await
        .unwrap();

        assert_eq!(advanced, 1);
        assert_eq!(head.block_number, 1);
        assert_eq!(head.block_hash, child_hash);
        assert_eq!(head.state_root, child_root);
        assert_eq!(factory.partial_state_root(&filter).unwrap(), child_root);
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
