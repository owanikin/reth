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
    BlockWriter, CanonChainTracker, ConfiguredContractFilter, ContractFilter, DBProvider,
    DatabaseProviderFactory, PartialStateRootProvider,
};
use reth_transaction_pool::test_utils::{testing_pool, TestPool};
use serde_json::{json, Value};

const TRACKED: Address = Address::repeat_byte(0x11);
const UNTRACKED: Address = Address::repeat_byte(0x22);
const EMPTY: Address = Address::repeat_byte(0x33);
const NESTED: Address = Address::repeat_byte(0x44);
const HASH_READER: Address = Address::repeat_byte(0x55);
const SENDER: Address = Address::repeat_byte(0x66);
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
