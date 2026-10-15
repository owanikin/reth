use crate::{eth::helpers::types::EthRpcConverter, EthApi};
use alloy_consensus::{constants::EMPTY_ROOT_HASH, Header};
use alloy_eips::BlockId;
use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
use reth_chainspec::{ChainSpec, DEV};
use reth_db_api::{
    models::StoredPartialStateCheckpoint,
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_ethereum_primitives::Block;
use reth_evm_ethereum::EthEvmConfig;
use reth_network_api::noop::NoopNetwork;
use reth_primitives_traits::{Account, Block as _, Bytecode, SealedHeader, StorageEntry};
use reth_provider::{
    providers::BlockchainProvider,
    test_utils::{create_test_provider_factory_with_chain_spec, MockNodeTypesWithDB},
    ProviderFactory,
};
use reth_rpc_eth_api::{helpers::LoadState, node::RpcNodeCoreAdapter, EthApiServer};
use reth_storage_api::{
    BlockReader, BlockWriter, CanonChainTracker, ConfiguredContractFilter, ContractFilter,
    DBProvider, DatabaseProviderFactory, PartialStateRootProvider,
};
use reth_transaction_pool::test_utils::{testing_pool, TestPool};
use serde_json::{json, Value};

const TRACKED: Address = Address::repeat_byte(0x11);
const UNTRACKED: Address = Address::repeat_byte(0x22);
const EMPTY: Address = Address::repeat_byte(0x33);
const NESTED: Address = Address::repeat_byte(0x44);
const HASH_READER: Address = Address::repeat_byte(0x55);
const SENDER: Address = Address::repeat_byte(0x66);
const UNTRACKED_STORAGE: Address = Address::repeat_byte(0x88);
const CODE: &[u8] = &[0x60, 0, 0x54, 0x60, 0, 0x52, 0x60, 0x20, 0x60, 0, 0xf3];

type TestEthApi = EthApi<
    RpcNodeCoreAdapter<
        BlockchainProvider<MockNodeTypesWithDB>,
        TestPool,
        NoopNetwork,
        EthEvmConfig,
    >,
    EthRpcConverter<ChainSpec>,
>;

struct Fixture {
    api: TestEthApi,
    factory: ProviderFactory<MockNodeTypesWithDB>,
    provider: BlockchainProvider<MockNodeTypesWithDB>,
    header: SealedHeader,
    parent: SealedHeader,
}

impl Fixture {
    fn new() -> Self {
        let factory = create_test_provider_factory_with_chain_spec(DEV.clone());
        let filter = ConfiguredContractFilter::new([TRACKED, NESTED, HASH_READER]);
        let rw = factory.database_provider_rw().unwrap();
        let tx = rw.tx_ref();

        // SLOAD(0), a nested CALL to untracked code, and BLOCKHASH(0).
        let mut nested = vec![0x60, 0, 0x60, 0, 0x60, 0, 0x60, 0, 0x60, 0, 0x73];
        nested.extend_from_slice(UNTRACKED.as_slice());
        nested.extend_from_slice(&[0x5a, 0xf1, 0x50, 0]);
        let hash_code = [0x60, 0, 0x40, 0x60, 0, 0x52, 0x60, 0x20, 0x60, 0, 0xf3];
        for (address, code) in
            [(TRACKED, CODE), (UNTRACKED, CODE), (NESTED, &nested), (HASH_READER, &hash_code)]
        {
            let code_hash = keccak256(code);
            tx.put::<tables::PartialStateAccounts>(
                keccak256(address),
                Account { nonce: 3, balance: U256::from(100), bytecode_hash: Some(code_hash) },
            )
            .unwrap();
            tx.put::<tables::Bytecodes>(
                code_hash,
                Bytecode::new_raw_checked(Bytes::copy_from_slice(code)).unwrap(),
            )
            .unwrap();
        }
        tx.put::<tables::PartialStateAccounts>(
            keccak256(EMPTY),
            Account { nonce: 7, balance: U256::from(200), bytecode_hash: None },
        )
        .unwrap();
        tx.put::<tables::PartialStateAccounts>(
            keccak256(SENDER),
            Account { balance: U256::from(10).pow(U256::from(20)), ..Default::default() },
        )
        .unwrap();
        // The reader must never replace this unavailable trie with an empty one.
        tx.put::<tables::PartialStateStorageRoots>(keccak256(UNTRACKED), B256::repeat_byte(0x99))
            .unwrap();
        // No code is needed to retain a storage commitment. Supplied call code can read this
        // account, isolating missing storage from the untracked-bytecode account guard.
        tx.put::<tables::PartialStateAccounts>(
            keccak256(UNTRACKED_STORAGE),
            Account { nonce: 1, ..Default::default() },
        )
        .unwrap();
        tx.put::<tables::PartialStateStorageRoots>(
            keccak256(UNTRACKED_STORAGE),
            B256::repeat_byte(0x98),
        )
        .unwrap();
        let slot = StorageEntry { key: keccak256(B256::ZERO), value: U256::from(42) };
        tx.put::<tables::PartialStateStorages>(keccak256(TRACKED), slot).unwrap();
        tx.put::<tables::PartialStateStorageRoots>(
            keccak256(TRACKED),
            reth_trie_common::root::storage_root([(slot.key, slot.value)]),
        )
        .unwrap();
        let root = rw.partial_state_root(&filter).unwrap();

        let parent = SealedHeader::seal_slow(Header {
            gas_limit: 30_000_000,
            state_root: EMPTY_ROOT_HASH,
            ..Default::default()
        });
        let header = SealedHeader::seal_slow(Header {
            parent_hash: parent.hash(),
            number: 1,
            timestamp: 1,
            gas_limit: 30_000_000,
            state_root: root,
            base_fee_per_gas: Some(0),
            excess_blob_gas: Some(0),
            blob_gas_used: Some(0),
            ..Default::default()
        });
        for h in [&parent, &header] {
            let block = Block { header: h.clone().unseal(), body: Default::default() };
            rw.insert_block(&block.try_into_recovered().unwrap()).unwrap();
            tx.put::<tables::PartialStateHeaders>(h.number, h.clone().unseal()).unwrap();
        }
        tx.put::<tables::PartialStateCheckpoints>(
            0,
            StoredPartialStateCheckpoint {
                block_number: 1,
                block_hash: header.hash(),
                state_root: root,
                sync_complete: true,
                filter_hash: filter.filter_hash(),
            },
        )
        .unwrap();
        rw.commit().unwrap();

        let provider = BlockchainProvider::with_latest(factory.clone(), header.clone()).unwrap();
        let api = Self::api(provider.clone(), true);
        Self { api, factory, provider, header, parent }
    }

    fn api(provider: BlockchainProvider<MockNodeTypesWithDB>, partial: bool) -> TestEthApi {
        EthApi::builder(
            provider,
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::new(DEV.clone()),
        )
        .partial_state(partial, [TRACKED, NESTED, HASH_READER])
        .build()
    }

    async fn rpc(&self, method: &str, params: Value) -> Value {
        rpc(&self.api, method, params).await
    }

    fn follower(&self) -> TestEthApi {
        Self::api(
            BlockchainProvider::with_latest(self.factory.clone(), self.parent.clone())
                .unwrap()
                .with_partial_state_follower(true),
            true,
        )
    }
}

async fn rpc(api: &TestEthApi, method: &str, params: Value) -> Value {
    let request = json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params});
    let module = api.clone().into_rpc();
    let (response, _) = module.raw_json_request(&request.to_string(), 1).await.unwrap();
    serde_json::from_str(response.get()).unwrap()
}

fn assert_error(response: &Value, code: i64, message: &str) {
    assert_eq!(response["error"]["code"], code, "{response}");
    assert!(response["error"]["message"].as_str().unwrap().contains(message), "{response}");
    assert!(response.get("result").is_none(), "{response}");
}

#[tokio::test]
async fn partial_state_follower_rpc_calls_checkpoint_and_rejects_unsupported_workloads() {
    let f = Fixture::new();
    let api = f.follower();
    let full = Fixture::new();
    let rw = full.factory.database_provider_rw().unwrap();
    for address in [TRACKED, UNTRACKED, EMPTY, NESTED, HASH_READER, SENDER, UNTRACKED_STORAGE] {
        let account =
            rw.tx_ref().get::<tables::PartialStateAccounts>(keccak256(address)).unwrap().unwrap();
        rw.tx_ref().put::<tables::PlainAccountState>(address, account).unwrap();
    }
    rw.tx_ref()
        .put::<tables::PlainStorageState>(TRACKED, StorageEntry::new(B256::ZERO, U256::from(42)))
        .unwrap();
    rw.commit().unwrap();
    let full_api = Fixture::api(full.provider.clone(), false);
    assert_eq!(rpc(&api, "eth_getBalance", json!([UNTRACKED, "latest"])).await["result"], "0x64");
    assert_eq!(
        rpc(&api, "eth_getCode", json!([TRACKED, "latest"])).await["result"],
        json!(Bytes::from_static(CODE))
    );
    for block in [json!("latest"), json!("0x1"), json!({"blockHash": f.header.hash()})] {
        for method in ["eth_call", "eth_estimateGas"] {
            let params = json!([{"from":SENDER,"to":TRACKED}, block]);
            let response = rpc(&api, method, params.clone()).await;
            assert!(response.get("error").is_none(), "{response}");
            assert_eq!(response, rpc(&full_api, method, params).await);
        }
    }
    assert!(rpc(&api, "eth_getBalance", json!([TRACKED, "0x0"])).await.get("error").is_some());
    let response = rpc(
        &api,
        "eth_simulateV1",
        json!([{"blockStateCalls": [{"calls": [{"to": TRACKED}]}]}, "latest"]),
    )
    .await;
    assert!(
        response["error"]["message"].as_str().unwrap().contains("read-only partial-state follower"),
        "{response}"
    );

    // Valid signed bytes must be refused even though this fixture uses an accepting test pool.
    let raw = "0x02f871018303579880850555633d1b82520894eee27662c2b8eba3cd936a23f039f3189633e4c887ad591c62bdaeb180c080a07ea72c68abfb8fca1bd964f0f99132ed9280261bdca3e549546c0205e800f7d0a05b4ef3039e9c9b9babc179a1878fb825b5aaf5aed2fa8744854150157b08d6f3";
    let response = rpc(&api, "eth_sendRawTransaction", json!([raw])).await;
    assert!(
        response["error"]["message"].as_str().unwrap().contains("read-only partial-state follower"),
        "{response}"
    );
}

#[tokio::test]
async fn partial_state_follower_rpc_uses_header_context_and_never_persists_calls() {
    let f = Fixture::new();
    let api = f.follower();
    // Return NUMBER and TIMESTAMP: the executed head is genesis, the checkpoint is block 1.
    let overrides = json!({(TRACKED.to_string()): {"code":"0x436000524260205260406000f3"}});
    let response =
        rpc(&api, "eth_call", json!([{"from":SENDER,"to":TRACKED}, "latest", overrides])).await;
    let mut expected = vec![0; 64];
    expected[31] = 1;
    expected[63] = 1;
    assert_eq!(response["result"], json!(Bytes::from(expected)), "{response}");
    let response = rpc(&api, "eth_call", json!([{"from":SENDER,"to":HASH_READER}, "latest"])).await;
    assert_eq!(response["result"], json!(f.parent.hash()), "{response}");

    // SSTORE(0, 99) affects only the simulation overlay, including during gas search.
    let overrides = json!({(TRACKED.to_string()): {"code":"0x606360005500"}});
    for method in ["eth_call", "eth_estimateGas"] {
        let response =
            rpc(&api, method, json!([{"from":SENDER,"to":TRACKED}, "latest", overrides])).await;
        assert!(response.get("error").is_none(), "{response}");
    }
    assert_eq!(
        rpc(&api, "eth_getStorageAt", json!([TRACKED, "0x0", "latest"])).await["result"],
        json!(B256::from(U256::from(42)))
    );
    let ro = f.factory.database_provider_ro().unwrap();
    assert_eq!(ro.tx_ref().entries::<tables::PlainAccountState>().unwrap(), 0);
    assert_eq!(ro.tx_ref().entries::<tables::PlainStorageState>().unwrap(), 0);
    assert_eq!(ro.tx_ref().entries::<tables::HashedAccounts>().unwrap(), 0);
    assert_eq!(ro.tx_ref().entries::<tables::HashedStorages>().unwrap(), 0);
    assert_eq!(
        ro.tx_ref().get::<tables::PartialStateCheckpoints>(0).unwrap().unwrap().block_hash,
        f.header.hash()
    );
}

#[tokio::test]
async fn partial_state_follower_rpc_rejects_unavailable_data_and_block_selections() {
    let f = Fixture::new();
    let api = f.follower();
    for method in ["eth_call", "eth_estimateGas"] {
        for target in [UNTRACKED, NESTED] {
            assert_error(
                &rpc(&api, method, json!([{"from":SENDER,"to":target},"latest"])).await,
                -32002,
                "not tracked",
            );
        }
        for block in [
            json!("pending"),
            json!("safe"),
            json!("finalized"),
            json!({"blockHash":f.header.hash(),"requireCanonical":true}),
        ] {
            assert_error(
                &rpc(&api, method, json!([{"from":SENDER,"to":TRACKED},block])).await,
                -32003,
                "not supported",
            );
        }
        for block in [json!("0x0"), json!({"blockHash": B256::repeat_byte(0xab)})] {
            assert_error(
                &rpc(&api, method, json!([{"from":SENDER,"to":TRACKED},block])).await,
                -32003,
                "outside",
            );
        }
    }
    let rw = f.factory.database_provider_rw().unwrap();
    rw.tx_ref().delete::<tables::Bytecodes>(keccak256(CODE), None).unwrap();
    rw.commit().unwrap();
    for method in ["eth_call", "eth_estimateGas"] {
        assert_error(
            &rpc(&api, method, json!([{"from":SENDER,"to":TRACKED},"latest"])).await,
            -32003,
            "unavailable",
        );
    }
}

#[tokio::test]
async fn partial_state_follower_rpc_missing_storage_and_nested_writes_are_not_persisted() {
    let f = Fixture::new();
    let api = f.follower();
    // SSTORE(0, 99), then CALL the untracked account and discard its success flag.
    let nested = |dependency: Address| {
        let mut code =
            vec![0x60, 99, 0x60, 0, 0x55, 0x60, 0, 0x60, 0, 0x60, 0, 0x60, 0, 0x60, 0, 0x73];
        code.extend_from_slice(dependency.as_slice());
        code.extend_from_slice(&[
            0x5a, 0xf1, 0x50, 0x60, 42, 0x60, 0, 0x52, 0x60, 32, 0x60, 0, 0xf3,
        ]);
        Bytes::from(code)
    };
    let at = json!({"blockHash": f.header.hash()});
    let checkpoint_before = f
        .factory
        .database_provider_ro()
        .unwrap()
        .tx_ref()
        .get::<tables::PartialStateCheckpoints>(0)
        .unwrap();
    let bytecodes_before =
        f.factory.database_provider_ro().unwrap().tx_ref().entries::<tables::Bytecodes>().unwrap();

    for method in ["eth_call", "eth_estimateGas"] {
        for (target, overrides, expected_code, message, missing_address) in [
            (
                UNTRACKED_STORAGE,
                json!({(UNTRACKED_STORAGE.to_string()): {"code":Bytes::from_static(CODE)}}),
                -32001,
                "storage",
                UNTRACKED_STORAGE,
            ),
            (
                TRACKED,
                json!({(TRACKED.to_string()): {"code":nested(UNTRACKED)}}),
                -32002,
                "bytecode",
                UNTRACKED,
            ),
            (
                TRACKED,
                json!({
                    (TRACKED.to_string()): {"code":nested(UNTRACKED_STORAGE)},
                    (UNTRACKED_STORAGE.to_string()): {"code":Bytes::from_static(CODE)},
                }),
                -32001,
                "storage",
                UNTRACKED_STORAGE,
            ),
            (
                UNTRACKED,
                json!({(UNTRACKED.to_string()): {"code":Bytes::from_static(CODE)}}),
                -32002,
                "bytecode",
                UNTRACKED,
            ),
        ] {
            let response =
                rpc(&api, method, json!([{"from":SENDER,"to":target},at,overrides])).await;
            assert_error(&response, expected_code, message);
            assert!(response["error"]["message"]
                .as_str()
                .unwrap()
                .to_lowercase()
                .contains(&missing_address.to_string().to_lowercase()));
            assert_eq!(
                rpc(&api, "eth_getStorageAt", json!([TRACKED, "0x0", at])).await["result"],
                json!(B256::from(U256::from(42)))
            );
            assert_eq!(
                rpc(&api, "eth_getCode", json!([TRACKED, at])).await["result"],
                json!(Bytes::from_static(CODE))
            );
            let valid = rpc(&api, method, json!([{"from":SENDER,"to":TRACKED},at])).await;
            assert!(valid.get("error").is_none(), "{valid}");
        }
    }
    let ro = f.factory.database_provider_ro().unwrap();
    assert_eq!(ro.tx_ref().get::<tables::PartialStateCheckpoints>(0).unwrap(), checkpoint_before);
    assert_eq!(ro.tx_ref().entries::<tables::Bytecodes>().unwrap(), bytecodes_before);
    assert_eq!(
        ro.partial_state_root(&ConfiguredContractFilter::new([TRACKED, NESTED, HASH_READER]))
            .unwrap(),
        f.header.state_root
    );
    assert_eq!(ro.tx_ref().entries::<tables::PlainAccountState>().unwrap(), 0);
    assert_eq!(ro.tx_ref().entries::<tables::PlainStorageState>().unwrap(), 0);
    assert_eq!(ro.tx_ref().entries::<tables::HashedAccounts>().unwrap(), 0);
    assert_eq!(ro.tx_ref().entries::<tables::HashedStorages>().unwrap(), 0);
}

#[tokio::test]
async fn partial_state_follower_rpc_state_diff_and_known_empty_are_not_missing() {
    let f = Fixture::new();
    let api = f.follower();
    let at = json!({"blockHash":f.header.hash()});
    let value = B256::from(U256::from(99));
    for method in ["eth_call", "eth_estimateGas"] {
        let response = rpc(
            &api,
            method,
            json!([
                {"from":SENDER,"to":TRACKED}, at,
                {(TRACKED.to_string()): {"stateDiff":{(B256::ZERO.to_string()):value}}},
            ]),
        )
        .await;
        assert!(response.get("error").is_none(), "{response}");
        if method == "eth_call" {
            assert_eq!(response["result"], json!(value));
        }
        for target in [EMPTY, Address::repeat_byte(0x77)] {
            let response = rpc(&api, method, json!([{"from":SENDER,"to":target},at])).await;
            assert!(response.get("error").is_none(), "{response}");
            if method == "eth_call" {
                assert_eq!(response["result"], "0x");
            }
        }
    }
    assert_eq!(
        rpc(&api, "eth_getStorageAt", json!([TRACKED, "0x0", at])).await["result"],
        json!(B256::from(U256::from(42)))
    );
}

#[tokio::test]
async fn partial_state_follower_rpc_requires_matching_complete_context() {
    let f = Fixture::new();
    let api = f.follower();
    let call = json!([{"from":SENDER,"to":TRACKED}, "latest"]);
    for method in ["eth_call", "eth_estimateGas"] {
        let rw = f.factory.database_provider_rw().unwrap();
        rw.tx_ref().delete::<tables::PartialStateHeaders>(1, None).unwrap();
        rw.commit().unwrap();
        // The matching normal header exists, but must not substitute for follower context.
        assert_error(&rpc(&api, method, call.clone()).await, -32003, "checkpoint header");
        let rw = f.factory.database_provider_rw().unwrap();
        rw.tx_ref().put::<tables::PartialStateHeaders>(1, f.parent.clone().unseal()).unwrap();
        rw.commit().unwrap();
        assert_error(&rpc(&api, method, call.clone()).await, -32003, "header mismatch");
    }
    f.factory.record_partial_state_header(&f.header).unwrap();
    for (complete, filter_hash, message) in [
        (
            false,
            ConfiguredContractFilter::new([TRACKED, NESTED, HASH_READER]).filter_hash(),
            "incomplete",
        ),
        (true, B256::ZERO, "filter mismatch"),
    ] {
        let rw = f.factory.database_provider_rw().unwrap();
        let mut cp = rw.tx_ref().get::<tables::PartialStateCheckpoints>(0).unwrap().unwrap();
        cp.sync_complete = complete;
        cp.filter_hash = filter_hash;
        rw.tx_ref().put::<tables::PartialStateCheckpoints>(0, cp).unwrap();
        rw.commit().unwrap();
        for method in ["eth_call", "eth_estimateGas"] {
            assert_error(&rpc(&api, method, call.clone()).await, -32003, message);
        }
    }
    let rw = f.factory.database_provider_rw().unwrap();
    rw.tx_ref().clear::<tables::PartialStateCheckpoints>().unwrap();
    rw.commit().unwrap();
    for method in ["eth_call", "eth_estimateGas"] {
        assert_error(&rpc(&api, method, call.clone()).await, -32003, "checkpoint is unavailable");
    }
}

#[tokio::test]
async fn partial_state_follower_rpc_pins_context_across_checkpoint_changes() {
    let f = Fixture::new();
    let provider = f.provider.clone().with_partial_state_follower(true);
    let filter = ConfiguredContractFilter::new([TRACKED, NESTED, HASH_READER]);
    let (header, snapshot) = provider.partial_state_call(BlockId::latest(), &filter).unwrap();
    let rw = f.factory.database_provider_rw().unwrap();
    // A reset/reorg after snapshot selection cannot replace its state or ancestor hashes.
    rw.tx_ref().clear::<tables::PartialStateCheckpoints>().unwrap();
    rw.tx_ref().clear::<tables::PartialStateStorages>().unwrap();
    rw.tx_ref().clear::<tables::PartialStateHeaders>().unwrap();
    rw.commit().unwrap();
    assert_eq!(header, f.header);
    assert_eq!(snapshot.storage(TRACKED, B256::ZERO).unwrap(), Some(U256::from(42)));
    assert_eq!(snapshot.block_hash(0).unwrap(), Some(f.parent.hash()));
    assert!(provider.partial_state_call(BlockId::latest(), &filter).is_err());
}

#[tokio::test]
async fn partial_state_follower_rpc_missing_ancestor_is_not_zero_or_full_state() {
    let f = Fixture::new();
    let mut checkpoint_header = f.header.clone().unseal();
    checkpoint_header.number = 2;
    checkpoint_header.parent_hash = f.header.hash();
    let checkpoint_header = SealedHeader::seal_slow(checkpoint_header);
    let rw = f.factory.database_provider_rw().unwrap();
    let mut cp = rw.tx_ref().get::<tables::PartialStateCheckpoints>(0).unwrap().unwrap();
    cp.block_number = 2;
    cp.block_hash = checkpoint_header.hash();
    rw.tx_ref().put::<tables::PartialStateCheckpoints>(0, cp).unwrap();
    rw.tx_ref().put::<tables::PartialStateHeaders>(2, checkpoint_header.unseal()).unwrap();
    // Genesis is still available from normal headers, but the follower's parent link is missing.
    rw.tx_ref().delete::<tables::PartialStateHeaders>(1, None).unwrap();
    rw.commit().unwrap();
    let api = f.follower();
    for method in ["eth_call", "eth_estimateGas"] {
        let response = rpc(&api, method, json!([{"from":SENDER,"to":HASH_READER},"latest"])).await;
        assert_error(&response, -32003, "BLOCKHASH history");
        let response = rpc(&api, method, json!([{"from":SENDER,"to":TRACKED},"latest"])).await;
        assert!(response.get("error").is_none(), "{response}");
    }

    // A header at the right height on another branch cannot supply its ancestor hashes.
    let rw = f.factory.database_provider_rw().unwrap();
    rw.tx_ref()
        .put::<tables::PartialStateHeaders>(
            1,
            Header { extra_data: Bytes::from_static(b"other branch"), ..f.header.clone_header() },
        )
        .unwrap();
    rw.commit().unwrap();
    assert_error(
        &rpc(&api, "eth_call", json!([{"to":HASH_READER},"latest"])).await,
        -32003,
        "BLOCKHASH history",
    );
}

#[tokio::test]
async fn partial_state_rpc_reads_checkpoint_without_full_state() {
    let f = Fixture::new();
    let ro = f.factory.database_provider_ro().unwrap();
    assert_eq!(ro.tx_ref().entries::<tables::PlainAccountState>().unwrap(), 0);
    assert_eq!(ro.tx_ref().entries::<tables::PlainStorageState>().unwrap(), 0);
    drop(ro);

    for block in [
        json!("latest"),
        json!("0x1"),
        json!({"blockHash": f.header.hash(), "requireCanonical":true}),
    ] {
        assert_eq!(f.rpc("eth_getBalance", json!([UNTRACKED, block])).await["result"], "0x64");
        assert_eq!(f.rpc("eth_getTransactionCount", json!([EMPTY, block])).await["result"], "0x7");
        assert_eq!(
            f.rpc("eth_getCode", json!([TRACKED, block])).await["result"],
            json!(Bytes::from_static(CODE))
        );
        assert_eq!(
            f.rpc("eth_getStorageAt", json!([TRACKED, "0x0", block])).await["result"],
            json!(B256::from(U256::from(42)))
        );
    }
    // Empty and nonexistent accounts are known empty, not missing state.
    for address in [EMPTY, Address::repeat_byte(0x77)] {
        assert_eq!(f.rpc("eth_getCode", json!([address, "latest"])).await["result"], "0x");
        assert_eq!(
            f.rpc("eth_getStorageAt", json!([address, "0x0", "latest"])).await["result"],
            json!(B256::ZERO)
        );
    }
    assert_error(&f.rpc("eth_getCode", json!([UNTRACKED, "latest"])).await, -32002, "not tracked");
    assert_error(
        &f.rpc("eth_getStorageAt", json!([UNTRACKED, "0x0", "latest"])).await,
        -32001,
        "not tracked",
    );
}

#[tokio::test]
async fn partial_state_rpc_executes_calls_and_estimates_without_full_state() {
    let f = Fixture::new();
    let call = json!({"from":SENDER, "to":TRACKED});
    let response = f.rpc("eth_call", json!([call, "latest"])).await;
    assert_eq!(response["result"], json!(B256::from(U256::from(42))), "{response}");
    let response = f.rpc("eth_estimateGas", json!([call, "latest"])).await;
    assert!(response.get("error").is_none(), "{response}");
    let gas: U256 = serde_json::from_value(response["result"].clone()).unwrap();
    assert!(gas > U256::from(21_000));
    assert_eq!(
        f.rpc("eth_call", json!([{"from":SENDER,"to":EMPTY}, "latest"])).await["result"],
        "0x"
    );
    let response = f.rpc("eth_call", json!([{"from":SENDER,"to":HASH_READER}, "latest"])).await;
    assert_eq!(response["result"], json!(f.parent.hash()), "{response}");

    for method in ["eth_call", "eth_estimateGas"] {
        for target in [UNTRACKED, NESTED] {
            assert_error(
                &f.rpc(method, json!([{"from":SENDER,"to":target}, "latest"])).await,
                -32002,
                "not tracked",
            );
        }
        assert_error(&f.rpc(method, json!([call, "pending"])).await, -32003, "pending");
    }
}

#[tokio::test]
async fn partial_state_rpc_rejects_incomplete_and_different_filter_checkpoints() {
    let f = Fixture::new();
    let rw = f.factory.database_provider_rw().unwrap();
    let mut checkpoint = rw.tx_ref().get::<tables::PartialStateCheckpoints>(0).unwrap().unwrap();
    checkpoint.sync_complete = false;
    rw.tx_ref().put::<tables::PartialStateCheckpoints>(0, checkpoint).unwrap();
    rw.commit().unwrap();
    assert_error(&f.rpc("eth_getCode", json!([TRACKED, "latest"])).await, -32003, "incomplete");

    let rw = f.factory.database_provider_rw().unwrap();
    checkpoint.sync_complete = true;
    checkpoint.filter_hash = ConfiguredContractFilter::default().filter_hash();
    rw.tx_ref().put::<tables::PartialStateCheckpoints>(0, checkpoint).unwrap();
    rw.commit().unwrap();
    assert_error(
        &f.rpc("eth_getBalance", json!([TRACKED, "latest"])).await,
        -32003,
        "filter mismatch",
    );
}

#[tokio::test]
async fn partial_state_rpc_rejects_missing_checkpoint_and_bytecode() {
    let f = Fixture::new();
    let rw = f.factory.database_provider_rw().unwrap();
    rw.tx_ref().delete::<tables::Bytecodes>(keccak256(CODE), None).unwrap();
    rw.commit().unwrap();
    assert_error(&f.rpc("eth_getCode", json!([TRACKED, "latest"])).await, -32003, "unavailable");
    assert_error(
        &f.rpc("eth_call", json!([{"to":TRACKED}, "latest"])).await,
        -32003,
        "unavailable",
    );

    let rw = f.factory.database_provider_rw().unwrap();
    rw.tx_ref().clear::<tables::PartialStateCheckpoints>().unwrap();
    // Full-state data must not mask a missing partial checkpoint.
    rw.tx_ref()
        .put::<tables::PlainAccountState>(
            TRACKED,
            Account { balance: U256::from(999), ..Default::default() },
        )
        .unwrap();
    rw.commit().unwrap();
    assert_error(
        &f.rpc("eth_getBalance", json!([TRACKED, "latest"])).await,
        -32003,
        "checkpoint is unavailable",
    );
    let full = Fixture::api(f.provider.clone(), false);
    assert_eq!(rpc(&full, "eth_getBalance", json!([TRACKED, "latest"])).await["result"], "0x3e7");
}

#[tokio::test]
async fn partial_state_rpc_rejects_other_blocks_pending_and_proofs() {
    let f = Fixture::new();
    assert_error(&f.rpc("eth_getBalance", json!([TRACKED, "0x0"])).await, -32003, "pivot mismatch");
    assert_error(&f.rpc("eth_getBalance", json!([TRACKED, "pending"])).await, -32003, "pending");
    assert_error(&f.rpc("eth_getProof", json!([TRACKED, [], "latest"])).await, -32003, "proofs");

    let snapshot = f.api.latest_state().unwrap();
    let mut next = f.header.clone().unseal();
    next.parent_hash = f.header.hash();
    next.number = 2;
    let next = SealedHeader::seal_slow(next);
    let rw = f.factory.database_provider_rw().unwrap();
    rw.insert_block(
        &Block { header: next.clone().unseal(), body: Default::default() }
            .try_into_recovered()
            .unwrap(),
    )
    .unwrap();
    rw.commit().unwrap();
    f.provider.set_canonical_head(next);
    assert_error(
        &f.rpc("eth_getBalance", json!([TRACKED, "latest"])).await,
        -32003,
        "pivot mismatch",
    );
    assert_eq!(f.rpc("eth_getBalance", json!([TRACKED, "0x1"])).await["result"], "0x64");
    assert_eq!(snapshot.block_hash(0).unwrap(), Some(f.parent.hash()));
    assert!(f.api.state_at_hash(f.parent.hash()).is_err());
    assert!(f.api.state_at_block_id(BlockId::pending()).await.is_err());
}
