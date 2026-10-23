//! Full-node state access and payload building after a canonical rewind to genesis.

use alloy_eips::eip4895::Withdrawal;
use alloy_primitives::{Address, B256, U256};
use alloy_rpc_types_engine::{ForkchoiceState, PayloadAttributes};
use reth_chainspec::{ChainSpecBuilder, EthChainSpec};
use reth_node_builder::{NodeBuilder, NodeConfig};
use reth_node_core::args::DatadirArgs;
use reth_node_ethereum::EthereumNode;
use reth_payload_builder::PayloadKind;
use reth_storage_api::{AccountReader, StateProviderFactory};
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn builds_payload_after_canonical_unwind_to_genesis() {
    let directory = tempfile::tempdir().unwrap();
    let address = Address::repeat_byte(1);
    let chain = Arc::new(
        ChainSpecBuilder::mainnet()
            .genesis(
                serde_json::from_value(serde_json::json!({
                    "gasLimit": "0x1c9c380",
                    "alloc": {
                        address.to_string(): {
                            "balance": "0x1",
                            "code": "0x60006000f3",
                            "storage": { B256::ZERO.to_string(): B256::with_last_byte(42) }
                        }
                    }
                }))
                .unwrap(),
            )
            .amsterdam_activated()
            .build(),
    );
    let mut config = NodeConfig::test().with_chain(chain.clone()).with_datadir_args(DatadirArgs {
        datadir: directory.path().join("data").into(),
        ..Default::default()
    });
    config.network.bootnodes = Some(Vec::new());
    config.network.discovery.disable_discovery = true;
    config.network.nat = "none".parse().unwrap();
    config.engine.allow_unwind_canonical_header = true;
    config.engine.always_process_payload_attributes_on_canonical_head = true;
    let handle = NodeBuilder::new(config)
        .with_database(reth_db::test_utils::create_test_rw_db())
        .with_launch_context(reth_tasks::Runtime::test())
        .node(EthereumNode::default())
        .launch()
        .await
        .unwrap();
    let node = &handle.node;
    let engine = &node.add_ons_handle.beacon_engine_handle;
    let genesis = chain.genesis_hash();
    let forkchoice = |hash| ForkchoiceState {
        head_block_hash: hash,
        safe_block_hash: genesis,
        finalized_block_hash: genesis,
    };
    let genesis_state = node.provider.state_by_block_hash(genesis).unwrap();
    let account = genesis_state.basic_account(&address).unwrap().unwrap();
    let code = genesis_state.bytecode_by_hash(&account.bytecode_hash.unwrap()).unwrap();
    assert!(code.is_some());
    assert_eq!(genesis_state.storage(address, B256::ZERO).unwrap(), Some(U256::from(42)));
    drop(genesis_state);

    let mut previous_hash = None;
    for amount in 1..=2 {
        // The second iteration rewinds block 1 to genesis, then repeats forkchoice there.
        for _ in 0..2 {
            let response = tokio::time::timeout(
                Duration::from_secs(10),
                engine.fork_choice_updated(forkchoice(genesis), None),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(response.is_valid(), "{response:?}");
            assert_eq!(node.provider.basic_account(&address).unwrap(), Some(account));
            for state in [
                node.provider.latest().expect("latest state after rewind"),
                node.provider.state_by_block_hash(genesis).expect("genesis state by hash"),
                node.provider.history_by_block_number(0).expect("genesis state by number"),
            ] {
                assert_eq!(state.basic_account(&address).unwrap(), Some(account));
                assert_eq!(state.storage(address, B256::ZERO).unwrap(), Some(U256::from(42)));
                assert_eq!(state.bytecode_by_hash(&account.bytecode_hash.unwrap()).unwrap(), code);
            }
        }

        let response = tokio::time::timeout(
            Duration::from_secs(10),
            engine.fork_choice_updated(
                forkchoice(genesis),
                Some(PayloadAttributes {
                    timestamp: chain.genesis().timestamp + amount,
                    withdrawals: Some(vec![Withdrawal {
                        index: 0,
                        validator_index: 0,
                        address,
                        amount,
                    }]),
                    parent_beacon_block_root: Some(B256::ZERO),
                    slot_number: Some(amount),
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
            node.payload_builder_handle
                .resolve_kind(response.payload_id.unwrap(), PayloadKind::WaitForPending),
        )
        .await
        .expect("payload building must not stall after a genesis rewind")
        .unwrap()
        .unwrap();
        let hash = payload.block().hash();
        assert_eq!(payload.block().number, 1);
        assert_eq!(payload.block().parent_hash, genesis);
        assert_ne!(Some(hash), previous_hash);
        previous_hash = Some(hash);
        let response =
            tokio::time::timeout(Duration::from_secs(10), engine.new_payload(payload.into()))
                .await
                .unwrap()
                .unwrap();
        assert!(response.is_valid(), "{response:?}");
        let response = tokio::time::timeout(
            Duration::from_secs(10),
            engine.fork_choice_updated(forkchoice(hash), None),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(response.is_valid(), "{response:?}");
        let balance =
            node.provider.latest().unwrap().basic_account(&address).unwrap().unwrap().balance;
        assert_eq!(balance, U256::from(1 + amount * 1_000_000_000));
    }

    let shutdown = node.task_executor.initiate_graceful_shutdown().unwrap();
    drop(tokio::time::timeout(Duration::from_secs(5), shutdown).await.unwrap());
    tokio::time::timeout(Duration::from_secs(5), handle.node_exit_future).await.unwrap().unwrap();
}
