//! Launches the opt-in follower without an execution engine or full-state backfill.

#[path = "follower/reorg.rs"]
mod reorg;

use alloy_eips::NumHash;
use alloy_primitives::{keccak256, Bytes, Sealed, B256};
use alloy_rpc_types_engine::{
    ExecutionData, ForkchoiceState, PayloadAttributes, PayloadStatusEnum,
};
use reth_chainspec::{ChainSpec, ChainSpecBuilder, EthChainSpec};
use reth_db_api::{tables, transaction::DbTx, Database};
use reth_eth_wire_types::{BlockAccessLists, EthVersion, GetAccountRangeMessage};
use reth_network_api::{BlockDownloaderProvider, NetworkInfo, Peers};
use reth_network_p2p::{
    error::RequestError,
    snap::client::{SnapClient, SnapResponse},
    BlockAccessListsClient,
};
use reth_node_api::BuiltPayload;
use reth_node_builder::{NodeBuilder, NodeConfig};
use reth_node_core::args::DatadirArgs;
use reth_node_ethereum::EthereumNode;
use reth_payload_builder::PayloadKind;
use reth_storage_api::{
    BalProvider, DatabaseProviderFactory, PartialStateSnapProvider, StateProviderFactory,
};
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn partial_state_follower_launcher_does_not_execute_or_persist_payloads() {
    let directory = tempfile::tempdir().unwrap();
    let chain = Arc::new(
        ChainSpecBuilder::mainnet().genesis(Default::default()).with_amsterdam_at(0).build(),
    );
    let mut toml = reth_config::Config::default();
    toml.partial_state.enabled = true;
    toml.partial_state.follower = true;
    toml.partial_state.trusted_checkpoint = Some(reth_config::PartialStateTrustedCheckpoint {
        chain_id: chain.chain().id(),
        genesis_hash: chain.genesis_hash(),
        block_number: 1,
        block_hash: B256::repeat_byte(1),
        state_root: B256::repeat_byte(2),
    });
    let path = directory.path().join("follower.toml");
    toml.save(&path).unwrap();
    let mut config =
        NodeConfig::test().with_chain(chain).with_config(path).with_datadir_args(DatadirArgs {
            datadir: directory.path().join("data").into(),
            ..Default::default()
        });
    config.network.bootnodes = Some(Vec::new());
    config.network.discovery.disable_discovery = true;
    config.network.nat = "none".parse().unwrap();
    let db = reth_db::test_utils::create_test_rw_db();
    let handle = NodeBuilder::new(config)
        .with_database(db.clone())
        .with_launch_context(reth_tasks::Runtime::test())
        .node(EthereumNode::default())
        .launch()
        .await
        .unwrap();
    assert!(handle.node.provider.is_partial_state_follower());
    assert!(handle.node.provider.latest().is_err());

    let counts = || {
        let tx = db.tx().unwrap();
        (
            tx.entries::<tables::PlainAccountState>().unwrap(),
            tx.entries::<tables::PlainStorageState>().unwrap(),
            tx.entries::<tables::HashedAccounts>().unwrap(),
            tx.entries::<tables::HashedStorages>().unwrap(),
            tx.entries::<tables::Headers>().unwrap(),
        )
    };
    let before = counts();
    let engine = &handle.node.add_ons_handle.beacon_engine_handle;
    let payload = ExecutionData::from_block_unchecked(
        B256::repeat_byte(3),
        &reth_ethereum_primitives::Block::default(),
    );
    let status = tokio::time::timeout(Duration::from_secs(5), engine.new_payload(payload))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.status, PayloadStatusEnum::Syncing);
    for _ in 0..3 {
        let response = tokio::time::timeout(
            Duration::from_secs(5),
            engine.fork_choice_updated(
                ForkchoiceState { head_block_hash: B256::repeat_byte(3), ..Default::default() },
                Some(PayloadAttributes::default()),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.payload_status.status, PayloadStatusEnum::Syncing);
        assert!(response.payload_status.latest_valid_hash.is_none());
        assert!(response.payload_id.is_none());
    }
    assert_eq!(counts(), before);
    let tx = db.tx().unwrap();
    assert_eq!(tx.entries::<tables::PartialStateCheckpoints>().unwrap(), 0);
    drop(tx);
    tokio::time::timeout(
        Duration::from_secs(5),
        handle.node.add_ons_handle.engine_shutdown.shutdown().unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle.node_exit_future).await.unwrap().unwrap();
}

#[tokio::test]
async fn partial_state_follower_negotiates_bal_exchange_with_opted_in_server() {
    assert_follower_bal_negotiation(true).await;
}

#[tokio::test]
async fn partial_state_follower_falls_back_without_bal_support() {
    assert_follower_bal_negotiation(false).await;
}

#[tokio::test]
async fn eth71_full_nodes_retain_and_serve_built_and_received_bals() {
    let chain = Arc::new(
        ChainSpecBuilder::mainnet()
            .genesis(
                serde_json::from_value(serde_json::json!({ "gasLimit": "0x1c9c380" })).unwrap(),
            )
            .amsterdam_activated()
            .build(),
    );
    let producer_dir = tempfile::tempdir().unwrap();
    let receiver_dir = tempfile::tempdir().unwrap();
    let mut nodes = Vec::new();
    for directory in [&producer_dir, &receiver_dir] {
        let mut config = isolated_network_config(chain.clone(), directory);
        config.network.enable_eth71 = true;
        assert!(!config.partial_state.enabled);
        nodes.push(
            NodeBuilder::new(config)
                .with_database(reth_db::test_utils::create_test_rw_db())
                .with_launch_context(reth_tasks::Runtime::test())
                .node(EthereumNode::default())
                .launch()
                .await
                .unwrap(),
        );
    }
    let producer = &nodes[0].node;
    let receiver = &nodes[1].node;
    let genesis = chain.genesis_hash();
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        producer.add_ons_handle.beacon_engine_handle.fork_choice_updated(
            ForkchoiceState {
                head_block_hash: genesis,
                safe_block_hash: genesis,
                finalized_block_hash: genesis,
            },
            Some(PayloadAttributes {
                timestamp: chain.genesis().timestamp + 1,
                withdrawals: Some(Vec::new()),
                parent_beacon_block_root: Some(B256::ZERO),
                slot_number: Some(1),
                ..Default::default()
            }),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(response.is_valid(), "{response:?}");
    let payload = tokio::time::timeout(
        Duration::from_secs(10),
        producer
            .payload_builder_handle
            .resolve_kind(response.payload_id.unwrap(), PayloadKind::WaitForPending),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    let hash = payload.block().hash();
    let bal = payload.block_access_list().expect("Amsterdam payload must contain a BAL").clone();

    // Check the builder event path before newPayload can populate the producer's store.
    tokio::time::timeout(Duration::from_secs(5), async {
        while producer.provider.bal_store().get_by_hash(hash).unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("full eth/71 producer must retain its locally built BAL");
    assert_eq!(producer.provider.bal_store().get_by_hash(hash).unwrap(), Some(bal.clone()));

    let response = tokio::time::timeout(
        Duration::from_secs(10),
        receiver.add_ons_handle.beacon_engine_handle.new_payload(payload.into()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(response.is_valid(), "{response:?}");
    assert_eq!(receiver.provider.bal_store().get_by_hash(hash).unwrap(), Some(bal.clone()));

    producer.network.add_peer(*receiver.network.peer_id(), receiver.network.local_addr());
    tokio::time::timeout(Duration::from_secs(10), async {
        while producer.network.get_all_peers().await.unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    for node in [producer, receiver] {
        assert!(!node.provider.is_partial_state_follower());
        let client = node.network.fetch_client().await.unwrap();
        let response =
            tokio::time::timeout(Duration::from_secs(5), client.get_block_access_lists(vec![hash]))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(response.into_data(), BlockAccessLists(vec![Some(bal.clone())]));
        let provider = node.provider.database_provider_ro().unwrap();
        assert_eq!(provider.tx_ref().entries::<tables::PartialStateCheckpoints>().unwrap(), 0);
    }
    for handle in nodes {
        let shutdown = handle.node.task_executor.initiate_graceful_shutdown().unwrap();
        drop(tokio::time::timeout(Duration::from_secs(5), shutdown).await.unwrap());
        tokio::time::timeout(Duration::from_secs(5), handle.node_exit_future)
            .await
            .unwrap()
            .unwrap();
    }
}

async fn assert_follower_bal_negotiation(server_eth71: bool) {
    let chain = Arc::new(
        ChainSpecBuilder::mainnet().genesis(Default::default()).with_amsterdam_at(0).build(),
    );
    let follower_dir = tempfile::tempdir().unwrap();
    let server_dir = tempfile::tempdir().unwrap();
    let mut toml = reth_config::Config::default();
    toml.partial_state.enabled = true;
    toml.partial_state.follower = true;
    // An unavailable seed keeps the worker idle while we exercise the network directly.
    toml.partial_state.trusted_checkpoint = Some(reth_config::PartialStateTrustedCheckpoint {
        chain_id: chain.chain().id(),
        genesis_hash: chain.genesis_hash(),
        block_number: 1,
        block_hash: B256::repeat_byte(1),
        state_root: B256::repeat_byte(2),
    });
    let path = follower_dir.path().join("follower.toml");
    toml.save(&path).unwrap();
    let follower_config = isolated_network_config(chain.clone(), &follower_dir).with_config(path);
    // TOML-only partial mode must enable BAL negotiation without a CLI override.
    assert!(!follower_config.network.enable_eth71);
    let follower = NodeBuilder::new(follower_config)
        .with_database(reth_db::test_utils::create_test_rw_db())
        .with_launch_context(reth_tasks::Runtime::test())
        .node(EthereumNode::default())
        .launch()
        .await
        .unwrap();
    let mut server_config = isolated_network_config(chain, &server_dir);
    server_config.network.enable_eth71 = server_eth71;
    let server = NodeBuilder::new(server_config)
        .with_database(reth_db::test_utils::create_test_rw_db())
        .with_launch_context(reth_tasks::Runtime::test())
        .node(EthereumNode::default())
        .launch()
        .await
        .unwrap();
    assert!(!server.node.provider.is_partial_state_follower());

    let hash = B256::repeat_byte(3);
    let bal = Bytes::from_static(&[0xc0]);
    server
        .node
        .provider
        .bal_store()
        .insert(NumHash::new(2, hash), Sealed::new_unchecked(bal.clone(), keccak256(&bal)))
        .unwrap();
    let follower_net = &follower.node.network;
    let server_net = &server.node.network;
    follower_net.add_peer(*server_net.peer_id(), server_net.local_addr());
    let expected_version = if server_eth71 { EthVersion::Eth71 } else { EthVersion::Eth69 };
    tokio::time::timeout(Duration::from_secs(15), async {
        for net in [follower_net, server_net] {
            loop {
                let peers = net.get_all_peers().await.unwrap();
                if let Some(peer) = peers.first() {
                    assert_eq!(peer.eth_version, expected_version);
                    assert!(peer.capabilities.supports_snap());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    })
    .await
    .expect("nodes should negotiate a session");

    let client = follower_net.fetch_client().await.unwrap();
    // Snap must still be routed correctly alongside either negotiated eth version.
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        client.get_account_range(GetAccountRangeMessage {
            request_id: 0,
            root_hash: server.node.provider.snap_state_pivot().unwrap().state_root,
            starting_hash: B256::ZERO,
            limit_hash: B256::repeat_byte(0xff),
            response_bytes: 1024,
        }),
    )
    .await
    .expect("snap request should complete")
    .unwrap()
    .into_data();
    assert!(matches!(response, SnapResponse::AccountRange(_)));
    let response =
        tokio::time::timeout(Duration::from_secs(5), client.get_block_access_lists(vec![hash]))
            .await
            .expect("BAL request must finish, including on a non-BAL session");
    if server_eth71 {
        assert_eq!(response.unwrap().into_data(), BlockAccessLists(vec![Some(bal)]));
    } else {
        assert_eq!(response.unwrap_err(), RequestError::UnsupportedCapability);
        // The follower advertises eth/71, but the full node negotiated eth/69 as well.
        let client = server_net.fetch_client().await.unwrap();
        let response =
            tokio::time::timeout(Duration::from_secs(5), client.get_block_access_lists(vec![hash]))
                .await
                .unwrap();
        assert_eq!(response.unwrap_err(), RequestError::UnsupportedCapability);
    }
    // Stop the follower frontend before shutting down its checkpoint worker.
    tokio::time::timeout(
        Duration::from_secs(5),
        follower.node.add_ons_handle.engine_shutdown.shutdown().unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    for handle in [follower, server] {
        let shutdown = handle.node.task_executor.initiate_graceful_shutdown().unwrap();
        drop(tokio::time::timeout(Duration::from_secs(5), shutdown).await.unwrap());
        tokio::time::timeout(Duration::from_secs(5), handle.node_exit_future)
            .await
            .unwrap()
            .unwrap();
    }
}

fn isolated_network_config(
    chain: Arc<ChainSpec>,
    directory: &tempfile::TempDir,
) -> NodeConfig<ChainSpec> {
    let mut config = NodeConfig::test().with_chain(chain).with_datadir_args(DatadirArgs {
        datadir: directory.path().join("data").into(),
        ..Default::default()
    });
    config.network.bootnodes = Some(Vec::new());
    config.network.discovery.disable_discovery = true;
    config.network.nat = "none".parse().unwrap();
    config.network.addr = std::net::Ipv4Addr::LOCALHOST.into();
    config
}
