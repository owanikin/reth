//! A real follower/HTTP server driven by a scripted eth/71 + snap peer. The fixture
//! supplies commitment-consistent branches, not execution-valid Ethereum blocks.

use alloy_consensus::Header;
use alloy_eips::{
    eip7928::{compute_block_access_list_hash, AccountChanges, SlotChanges, StorageChange},
    BlockHashOrNumber,
};
use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
use alloy_rpc_types_engine::{ForkchoiceState, PayloadAttributes, PayloadStatusEnum};
use jsonrpsee::{core::client::ClientT, http_client::HttpClient, rpc_params};
use reth_chainspec::{ChainSpec, ChainSpecBuilder, EthChainSpec};
use reth_db_api::{cursor::DbCursorRO, table::Table, tables, transaction::DbTx, Database};
use reth_eth_wire_types::{
    snap::{AccountData, AccountRangeMessage, ByteCodesMessage, StorageData, StorageRangesMessage},
    BlockAccessLists, BlockHeaders, EthVersion,
};
use reth_network::{
    eth_requests::IncomingEthRequest, HelloMessageWithProtocols, NetworkConfig, NetworkManager,
};
use reth_network_api::{NetworkInfo, Peers};
use reth_node_builder::NodeBuilder;
use reth_node_ethereum::EthereumNode;
use reth_primitives_traits::Account;
use reth_storage_api::{ConfiguredContractFilter, ContractFilter, PartialStateRootProvider};
use reth_trie::root::{state_root_unhashed, storage_root};
use reth_trie_common::{proof::ProofRetainer, HashBuilder, Nibbles, TrieAccount};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinSet, time::timeout};

#[path = "restart.rs"]
mod restart;

const TRACKED: Address = Address::repeat_byte(0x11);
const UNTRACKED: Address = Address::repeat_byte(0x22);
const WAIT: Duration = Duration::from_secs(30);
// Return SLOAD(0), NUMBER, TIMESTAMP and BLOCKHASH(NUMBER - 1).
const CODE: &[u8] = &[
    0x60, 0, 0x54, 0x60, 0, 0x52, 0x43, 0x60, 0x20, 0x52, 0x42, 0x60, 0x40, 0x52, 0x60, 1, 0x43,
    0x03, 0x40, 0x60, 0x60, 0x52, 0x60, 0x80, 0x60, 0, 0xf3,
];
const UNTRACKED_CODE: &[u8] = &[0x60, 0x01, 0x00];

struct BranchBlock {
    header: Header,
    bal: Bytes,
    slots: [u64; 2],
    untracked_slot: u64,
}

impl BranchBlock {
    fn new(parent: &Header, slots: [u64; 2], untracked_slot: u64, previous: [u64; 2]) -> Self {
        let mut tracked = AccountChanges::new(TRACKED);
        for (slot, (&value, old)) in slots.iter().zip(previous).enumerate() {
            if value != old {
                tracked = tracked.with_storage_change(SlotChanges::new(
                    U256::from(slot),
                    vec![StorageChange::new(1, U256::from(value))],
                ));
            }
        }
        let bal = vec![
            tracked,
            AccountChanges::new(UNTRACKED).with_storage_change(SlotChanges::new(
                U256::ZERO,
                vec![StorageChange::new(1, U256::from(untracked_slot))],
            )),
        ];
        let mut block = Self {
            header: Header {
                number: parent.number + 1,
                parent_hash: parent.hash_slow(),
                timestamp: parent.timestamp + 12,
                gas_limit: 30_000_000,
                base_fee_per_gas: Some(0),
                withdrawals_root: Some(alloy_consensus::EMPTY_ROOT_HASH),
                excess_blob_gas: Some(0),
                blob_gas_used: Some(0),
                parent_beacon_block_root: Some(B256::ZERO),
                requests_hash: Some(alloy_eips::eip7685::EMPTY_REQUESTS_HASH),
                block_access_list_hash: Some(compute_block_access_list_hash(&bal)),
                ..Default::default()
            },
            bal: alloy_rlp::encode(&bal).into(),
            slots,
            untracked_slot,
        };
        // Compute the oracle from the full fixture state, never from the follower's tables.
        block.header.state_root = state_root_unhashed(block.accounts());
        assert_eq!(
            alloy_rlp::decode_exact::<Header>(alloy_rlp::encode(&block.header)).unwrap(),
            block.header
        );
        block
    }

    fn accounts(&self) -> [(Address, TrieAccount); 2] {
        [
            (
                TRACKED,
                TrieAccount {
                    nonce: 1,
                    balance: U256::from(100),
                    code_hash: keccak256(CODE),
                    storage_root: storage_root(self.storage()),
                },
            ),
            (
                UNTRACKED,
                TrieAccount {
                    nonce: 1,
                    balance: U256::from(200),
                    code_hash: keccak256(UNTRACKED_CODE),
                    storage_root: storage_root([(
                        keccak256(B256::ZERO),
                        U256::from(self.untracked_slot),
                    )]),
                },
            ),
        ]
    }

    fn storage(&self) -> Vec<(B256, U256)> {
        let mut entries: Vec<_> = self
            .slots
            .iter()
            .enumerate()
            .filter(|(_, value)| **value != 0)
            .map(|(slot, &value)| {
                (keccak256(U256::from(slot).to_be_bytes::<32>()), U256::from(value))
            })
            .collect();
        entries.sort_by_key(|(hash, _)| *hash);
        entries
    }

    fn account_range(&self, start: B256, limit: B256, id: u64) -> AccountRangeMessage {
        let mut accounts: Vec<_> = self
            .accounts()
            .into_iter()
            .map(|(addr, account)| AccountData {
                hash: keccak256(addr),
                body: alloy_rlp::encode(account).into(),
            })
            .collect();
        accounts.sort_by_key(|account| account.hash);
        let path = Nibbles::unpack(start);
        let mut builder =
            HashBuilder::default().with_proof_retainer(ProofRetainer::from_iter([path]));
        for account in &accounts {
            builder.add_leaf(Nibbles::unpack(account.hash), &account.body);
        }
        assert_eq!(builder.root(), self.header.state_root);
        let proof = if start == limit {
            builder
                .take_proof_nodes()
                .matching_nodes_sorted(&path)
                .into_iter()
                .map(|(_, node)| node)
                .collect()
        } else {
            Vec::new()
        };
        accounts.retain(|account| account.hash >= start && account.hash <= limit);
        AccountRangeMessage { request_id: id, accounts, proof }
    }

    fn call_result(&self) -> Bytes {
        let mut result = Vec::new();
        for value in [self.slots[0], self.header.number, self.header.timestamp] {
            result.extend_from_slice(&U256::from(value).to_be_bytes::<32>());
        }
        result.extend_from_slice(self.header.parent_hash.as_slice());
        result.into()
    }
}

#[derive(Default)]
struct PeerRequests {
    snap_downloads: AtomicUsize,
    account_proofs: AtomicUsize,
    bals: Mutex<Vec<B256>>,
}

async fn serve(
    mut requests: mpsc::Receiver<IncomingEthRequest>,
    blocks: Arc<Vec<BranchBlock>>,
    observed: Arc<PeerRequests>,
) {
    while let Some(request) = requests.recv().await {
        match request {
            IncomingEthRequest::GetBlockHeaders { request, response, .. } => {
                assert_eq!(request.limit, 1);
                let BlockHashOrNumber::Hash(hash) = request.start_block else {
                    panic!("follower must request headers by hash")
                };
                let header = blocks.iter().find(|block| block.header.hash_slow() == hash).unwrap();
                response.send(Ok(BlockHeaders(vec![header.header.clone()]))).unwrap();
            }
            IncomingEthRequest::GetBlockAccessLists { request, response, .. } => {
                observed.bals.lock().unwrap().extend_from_slice(&request.0);
                let bals = request
                    .0
                    .iter()
                    .map(|hash| {
                        Some(
                            blocks
                                .iter()
                                .find(|block| block.header.hash_slow() == *hash)
                                .unwrap()
                                .bal
                                .clone(),
                        )
                    })
                    .collect();
                response.send(Ok(BlockAccessLists(bals))).unwrap();
            }
            IncomingEthRequest::GetAccountRange { request, response, .. } => {
                if request.starting_hash == request.limit_hash {
                    observed.account_proofs.fetch_add(1, Ordering::Relaxed);
                } else {
                    observed.snap_downloads.fetch_add(1, Ordering::Relaxed);
                }
                let block = blocks
                    .iter()
                    .find(|block| block.header.state_root == request.root_hash)
                    .unwrap();
                response
                    .send(Ok(block.account_range(
                        request.starting_hash,
                        request.limit_hash,
                        request.request_id,
                    )))
                    .unwrap();
            }
            IncomingEthRequest::GetStorageRanges { request, response, .. } => {
                observed.snap_downloads.fetch_add(1, Ordering::Relaxed);
                assert_eq!(
                    request.account_hashes,
                    [keccak256(TRACKED)],
                    "must not download untracked slots"
                );
                let block = blocks
                    .iter()
                    .find(|block| block.header.state_root == request.root_hash)
                    .unwrap();
                let slots = block
                    .storage()
                    .into_iter()
                    .filter(|(key, _)| *key >= request.starting_hash && *key <= request.limit_hash)
                    .map(|(hash, value)| StorageData {
                        hash,
                        data: alloy_rlp::encode(value).into(),
                    })
                    .collect();
                response
                    .send(Ok(StorageRangesMessage {
                        request_id: request.request_id,
                        slots: vec![slots],
                        proof: Vec::new(),
                    }))
                    .unwrap();
            }
            IncomingEthRequest::GetByteCodes { request, response, .. } => {
                observed.snap_downloads.fetch_add(1, Ordering::Relaxed);
                assert_eq!(request.hashes, [keccak256(CODE)], "must not download untracked code");
                response
                    .send(Ok(ByteCodesMessage {
                        request_id: request.request_id,
                        codes: vec![Bytes::from_static(CODE)],
                    }))
                    .unwrap();
            }
            other => panic!("unexpected request: {other:?}"),
        }
    }
}

fn rows<T: Table>(tx: &impl DbTx) -> Vec<(T::Key, T::Value)> {
    tx.cursor_read::<T>().unwrap().walk(None).unwrap().collect::<Result<_, _>>().unwrap()
}

async fn assert_rpc_state(client: &HttpClient, block: &BranchBlock) {
    let hash = block.header.hash_slow();
    for at in
        [json!("latest"), json!(format!("0x{:x}", block.header.number)), json!({"blockHash": hash})]
    {
        for (slot, value) in block.slots.iter().enumerate() {
            let actual: B256 = client
                .request("eth_getStorageAt", rpc_params![TRACKED, U256::from(slot), at.clone()])
                .await
                .unwrap();
            assert_eq!(actual, B256::from(U256::from(*value).to_be_bytes::<32>()));
        }
        let call = json!({"to": TRACKED, "gasPrice": "0x0"});
        let actual: Bytes =
            client.request("eth_call", rpc_params![call.clone(), at.clone()]).await.unwrap();
        assert_eq!(
            actual,
            block.call_result(),
            "storage and EVM header context must follow the checkpoint"
        );
        let gas: U256 =
            client.request("eth_estimateGas", rpc_params![call, at.clone()]).await.unwrap();
        assert!(gas >= U256::from(21_000));
        let code: Bytes =
            client.request("eth_getCode", rpc_params![TRACKED, at.clone()]).await.unwrap();
        assert_eq!(code.as_ref(), CODE);
        for (method, params, expected) in [
            ("eth_getCode", rpc_params![UNTRACKED, at.clone()], -32002),
            ("eth_getStorageAt", rpc_params![UNTRACKED, "0x0", at.clone()], -32001),
            ("eth_call", rpc_params![json!({"to": UNTRACKED}), at.clone()], -32002),
            ("eth_estimateGas", rpc_params![json!({"to": UNTRACKED}), at.clone()], -32002),
        ] {
            let err = client.request::<Value, _>(method, params).await.unwrap_err();
            let jsonrpsee::core::client::Error::Call(err) = err else { panic!("{err}") };
            assert_eq!(err.code(), expected);
            assert!(err.message().to_lowercase().contains(&UNTRACKED.to_string().to_lowercase()));
            assert!(err.message().contains("not tracked"));
        }
    }
    let executed: U256 = client.request("eth_blockNumber", rpc_params![]).await.unwrap();
    assert_eq!(executed, U256::ZERO);
}

#[test]
fn partial_state_follower_rpc_state_across_reorgs() {
    // Keep files alive until background RPC/provider tasks have been dropped with the runtime.
    let directory = tempfile::tempdir().unwrap();
    let runtime =
        tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
    runtime.block_on(assert_reorg_rpc(&directory));
}

fn fixture_chain() -> Arc<ChainSpec> {
    let genesis = serde_json::from_value(json!({
        "gasLimit": "0x1c9c380",
        // Different genesis storage detects accidental fallback to normal state tables.
        "alloc": {
            TRACKED.to_string(): {
                "balance": "0x64",
                "nonce": "0x1",
                "code": Bytes::from_static(CODE),
                "storage": {B256::ZERO.to_string(): B256::with_last_byte(99)}
            },
            UNTRACKED.to_string(): {"balance": "0xc8", "code": "0x00"}
        }
    }))
    .unwrap();
    Arc::new(ChainSpecBuilder::mainnet().genesis(genesis).amsterdam_activated().build())
}

fn fixture_branches(chain: &ChainSpec) -> Arc<Vec<BranchBlock>> {
    let seed = BranchBlock::new(chain.genesis_header(), [10, 0], 100, [0, 0]);
    let a1 = BranchBlock::new(&seed.header, [11, 111], 101, seed.slots);
    let a2 = BranchBlock::new(&a1.header, [12, 112], 102, a1.slots);
    let b1 = BranchBlock::new(&seed.header, [21, 0], 201, seed.slots);
    let b2 = BranchBlock::new(&b1.header, [22, 0], 202, b1.slots);
    let b3 = BranchBlock::new(&b2.header, [23, 333], 203, b2.slots);
    Arc::new(vec![seed, a1, a2, b1, b2, b3])
}

async fn assert_reorg_rpc(directory: &tempfile::TempDir) {
    reth_tracing::init_test_tracing();
    let chain = fixture_chain();
    let blocks = fixture_branches(&chain);
    let seed = &blocks[0];

    let mut builder = NetworkConfig::builder_with_rng_secret_key(reth_tasks::Runtime::test())
        .listener_addr("127.0.0.1:0".parse().unwrap())
        .disable_discovery()
        .disable_nat();
    let mut hello = HelloMessageWithProtocols::builder(builder.get_peer_id()).build();
    hello.try_add_protocol(EthVersion::Eth71.into()).unwrap();
    builder = builder.hello_message(hello);
    let mut network =
        NetworkManager::eth(builder.build_with_noop_provider(chain.clone())).await.unwrap();
    let peer = network.handle().clone();
    let (sender, requests) = mpsc::channel(32);
    network.set_eth_request_handler(sender);
    // JoinSet aborts the peer tasks on assertion failure as well as on normal teardown.
    let mut tasks = JoinSet::new();
    tasks.spawn(network);
    tasks.spawn(serve(requests, blocks.clone(), Arc::new(PeerRequests::default())));

    let mut toml = reth_config::Config::default();
    toml.partial_state.enabled = true;
    toml.partial_state.follower = true;
    toml.partial_state.contracts = [TRACKED].into();
    toml.partial_state.bal_retention = 64;
    toml.partial_state.trusted_checkpoint = Some(reth_config::PartialStateTrustedCheckpoint {
        chain_id: chain.chain().id(),
        genesis_hash: chain.genesis_hash(),
        block_number: seed.header.number,
        block_hash: seed.header.hash_slow(),
        state_root: seed.header.state_root,
    });
    let path = directory.path().join("follower.toml");
    toml.save(&path).unwrap();
    let mut config = super::isolated_network_config(chain, directory).with_config(path);
    config.rpc.http = true;
    config.rpc.http_api = Some("eth".parse().unwrap());
    let db = reth_db::test_utils::create_test_rw_db();
    let handle = NodeBuilder::new(config)
        .with_database(db.clone())
        .with_launch_context(reth_tasks::Runtime::test())
        .node(EthereumNode::default())
        .launch()
        .await
        .unwrap();
    let client = handle.node.rpc_server_handle().http_client().unwrap();
    let read_checkpoint = || db.tx().unwrap().get::<tables::PartialStateCheckpoints>(0).unwrap();
    let filter = ConfiguredContractFilter::new([TRACKED]);
    let full_state = || {
        let tx = db.tx().unwrap();
        (
            rows::<tables::PlainAccountState>(&tx),
            rows::<tables::PlainStorageState>(&tx),
            rows::<tables::HashedAccounts>(&tx),
            rows::<tables::HashedStorages>(&tx),
            rows::<tables::StageCheckpoints>(&tx),
        )
    };
    let before = full_state();
    assert_eq!(before.2.len(), 2, "genesis accounts must be present in the hashed state oracle");
    assert!(!before.3.is_empty(), "genesis storage must differ from the partial-state fixture");
    assert!(before.4.iter().all(|(_, checkpoint)| checkpoint.block_number == 0));
    let partial_state = || {
        let tx = db.tx().unwrap();
        (
            rows::<tables::PartialStateAccounts>(&tx),
            rows::<tables::PartialStateStorages>(&tx),
            rows::<tables::PartialStateStorageRoots>(&tx),
            rows::<tables::PartialStateHeaders>(&tx),
            rows::<tables::PartialStateTransitionJournals>(&tx),
            rows::<tables::Bytecodes>(&tx),
        )
    };
    handle.node.network.add_peer(*peer.peer_id(), peer.local_addr());
    timeout(WAIT, async {
        loop {
            if read_checkpoint()
                .is_some_and(|cp| cp.sync_complete && cp.filter_hash == filter.filter_hash())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("follower must bootstrap from the scripted peer");
    assert_eq!(read_checkpoint().unwrap().block_hash, seed.header.hash_slow());

    // Same-height replacement, extension, rewind, and replay of the original branch.
    for (index, stale) in [(2, None), (4, Some(2)), (5, Some(4)), (3, Some(5)), (2, Some(3))] {
        let block = &blocks[index];
        let hash = block.header.hash_slow();
        let engine = &handle.node.add_ons_handle.beacon_engine_handle;
        let response = timeout(
            WAIT,
            engine.fork_choice_updated(
                ForkchoiceState { head_block_hash: hash, ..Default::default() },
                Some(PayloadAttributes::default()),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.payload_status.status, PayloadStatusEnum::Syncing);
        assert!(response.payload_status.latest_valid_hash.is_none());
        assert!(response.payload_id.is_none());
        timeout(WAIT, async {
            loop {
                if read_checkpoint().is_some_and(|cp| cp.block_hash == hash) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("checkpoint did not reach {hash}: {:?}", read_checkpoint()));
        let checkpoint = read_checkpoint().unwrap();
        assert!(checkpoint.sync_complete);
        assert_eq!(checkpoint.filter_hash, filter.filter_hash());
        assert_eq!(checkpoint.block_number, block.header.number);
        assert_eq!(checkpoint.state_root, block.header.state_root);
        assert_eq!(
            handle.node.provider.partial_state_root(&filter).unwrap(),
            block.header.state_root
        );
        let before_rpc = partial_state();
        timeout(WAIT, assert_rpc_state(&client, block)).await.unwrap();
        if let Some(stale) = stale {
            let old_hash = blocks[stale].header.hash_slow();
            let at = json!({"blockHash": old_hash});
            for (method, params) in [
                ("eth_getCode", rpc_params![TRACKED, at.clone()]),
                ("eth_getStorageAt", rpc_params![TRACKED, "0x0", at.clone()]),
                ("eth_call", rpc_params![json!({"to": TRACKED}), at.clone()]),
                ("eth_estimateGas", rpc_params![json!({"to": TRACKED}), at]),
            ] {
                let err = timeout(WAIT, client.request::<Value, _>(method, params))
                    .await
                    .unwrap()
                    .unwrap_err();
                let jsonrpsee::core::client::Error::Call(err) = err else { panic!("{err}") };
                assert!(err.message().contains("checkpoint"), "{method}: {err}");
            }
        }
        assert_eq!(partial_state(), before_rpc, "RPC calls changed the persisted partial state");
        assert_eq!(read_checkpoint().unwrap(), checkpoint, "RPC calls changed the checkpoint");
        let tx = db.tx().unwrap();
        assert_eq!(
            tx.get::<tables::PartialStateHeaders>(block.header.number).unwrap(),
            Some(block.header.clone())
        );
        assert_eq!(
            tx.get::<tables::PartialStateAccounts>(keccak256(UNTRACKED)).unwrap(),
            Some(Account::from(block.accounts()[1].1))
        );
        assert_eq!(
            tx.get::<tables::PartialStateStorageRoots>(keccak256(UNTRACKED)).unwrap(),
            Some(block.accounts()[1].1.storage_root)
        );
        assert!(rows::<tables::PartialStateStorages>(&tx)
            .iter()
            .all(|(address, _)| *address == keccak256(TRACKED)));
        assert!(tx.get::<tables::Bytecodes>(keccak256(UNTRACKED_CODE)).unwrap().is_none());
        drop(tx);
        assert_eq!(full_state(), before, "normal full state or execution checkpoints changed");
        let response = timeout(
            WAIT,
            engine.fork_choice_updated(
                ForkchoiceState { head_block_hash: hash, ..Default::default() },
                None,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.payload_status.status, PayloadStatusEnum::Syncing);
        assert!(response.payload_status.latest_valid_hash.is_none());
        assert!(response.payload_id.is_none());
    }
    timeout(WAIT, handle.node.add_ons_handle.engine_shutdown.shutdown().unwrap())
        .await
        .unwrap()
        .unwrap();
    let shutdown = handle.node.task_executor.initiate_graceful_shutdown().unwrap();
    drop(timeout(WAIT, shutdown).await.unwrap());
    timeout(WAIT, handle.node_exit_future).await.unwrap().unwrap();
    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        assert!(result.is_ok() || result.unwrap_err().is_cancelled(), "scripted peer panicked");
    }
}
