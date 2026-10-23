//! Restart the real follower over the same MDBX/static-file directories, with no
//! surviving runtime, provider, network cache, or in-memory BAL store.

use super::*;

const RETENTION: u64 = 64;

#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    full: Value,
    partial: Value,
}

impl Snapshot {
    fn read(db: &impl Database) -> Self {
        let tx = db.tx().unwrap();
        Self {
            full: json!({
                "accounts": rows::<tables::PlainAccountState>(&tx),
                "storage": rows::<tables::PlainStorageState>(&tx),
                "hashed_accounts": rows::<tables::HashedAccounts>(&tx),
                "hashed_storage": rows::<tables::HashedStorages>(&tx),
                "stages": rows::<tables::StageCheckpoints>(&tx),
                "headers": rows::<tables::Headers>(&tx),
                "canonical_headers": rows::<tables::CanonicalHeaders>(&tx),
            }),
            partial: json!({
                "accounts": rows::<tables::PartialStateAccounts>(&tx),
                "storage": rows::<tables::PartialStateStorages>(&tx),
                "availability": rows::<tables::PartialStateAvailability>(&tx),
                "roots": rows::<tables::PartialStateStorageRoots>(&tx),
                "checkpoint": rows::<tables::PartialStateCheckpoints>(&tx),
                "headers": rows::<tables::PartialStateHeaders>(&tx),
                "journals": rows::<tables::PartialStateTransitionJournals>(&tx),
                "account_changes": rows::<tables::PartialStateAccountChangeSets>(&tx),
                "storage_changes": rows::<tables::PartialStateStorageChangeSets>(&tx),
                "availability_changes": rows::<tables::PartialStateAvailabilityChangeSets>(&tx),
                "code": rows::<tables::Bytecodes>(&tx),
            }),
        }
    }
}

#[test]
fn partial_state_follower_restart_replays_offline_reorg() {
    assert_restart(2);
}

#[test]
fn partial_state_follower_restart_rejects_reorg_beyond_retention() {
    assert_restart(RETENTION + 1);
}

fn assert_restart(reorg_depth: u64) {
    reth_tracing::init_test_tracing();
    let directory = tempfile::tempdir().unwrap();
    let chain = fixture_chain();
    let mut blocks = fixture_branches(&chain);
    let mut old_tip = 2;
    // Extend A past the minimum supported retention window so pruning happens through
    // the worker itself, not by deleting journals or bypassing configuration validation.
    for index in 0..reorg_depth - 2 {
        let blocks = Arc::get_mut(&mut blocks).unwrap();
        let parent = &blocks[old_tip];
        let child = BranchBlock::new(
            &parent.header,
            [1000 + index, 2000 + index],
            3000 + index,
            parent.slots,
        );
        blocks.push(child);
        old_tip = blocks.len() - 1;
    }
    let seed = &blocks[0].header;
    let mut toml = reth_config::Config::default();
    toml.partial_state.enabled = true;
    toml.partial_state.follower = true;
    toml.partial_state.contracts = [TRACKED].into();
    toml.partial_state.bal_retention = RETENTION;
    toml.partial_state.trusted_checkpoint = Some(reth_config::PartialStateTrustedCheckpoint {
        chain_id: chain.chain().id(),
        genesis_hash: chain.genesis_hash(),
        block_number: seed.number,
        block_hash: seed.hash_slow(),
        state_root: seed.state_root,
    });
    toml.save(&directory.path().join("follower.toml")).unwrap();

    let phase = |saved: Option<&Snapshot>| {
        // Runtime destruction drops background RPC/provider handles before reopening the DB.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(run_phase(&directory, chain.clone(), blocks.clone(), old_tip, saved))
    };
    let saved = phase(None);
    let recovered = phase(Some(&saved));
    let db =
        reth_db::open_db(directory.path().join("db"), reth_db::mdbx::DatabaseArguments::test())
            .unwrap();
    assert_eq!(Snapshot::read(&db), recovered, "recovery result did not survive shutdown");
}

async fn wait_for(mut condition: impl FnMut() -> bool, message: &str) {
    timeout(WAIT, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect(message);
}

async fn assert_unavailable_checkpoint(client: &HttpClient, hash: B256) {
    let at = json!({"blockHash": hash});
    for (method, params) in [
        ("eth_getCode", rpc_params![TRACKED, at.clone()]),
        ("eth_getStorageAt", rpc_params![TRACKED, "0x0", at.clone()]),
        ("eth_call", rpc_params![json!({"to": TRACKED}), at.clone()]),
        ("eth_estimateGas", rpc_params![json!({"to": TRACKED}), at]),
    ] {
        let err = client.request::<Value, _>(method, params).await.unwrap_err();
        let jsonrpsee::core::client::Error::Call(err) = err else { panic!("{err}") };
        assert!(err.message().contains("checkpoint"), "{method}: {err}");
    }
}

async fn run_phase(
    directory: &tempfile::TempDir,
    chain: Arc<ChainSpec>,
    blocks: Arc<Vec<BranchBlock>>,
    old_tip: usize,
    saved: Option<&Snapshot>,
) -> Snapshot {
    let restarted = saved.is_some();
    let pruned_history = blocks[old_tip].header.number - blocks[0].header.number > RETENTION;
    let insufficient_history = restarted && pruned_history;
    let path = directory.path().join("db");
    let args = reth_db::mdbx::DatabaseArguments::test();
    let db = Arc::new(if restarted {
        reth_db::open_db(path, args).unwrap()
    } else {
        reth_db::init_db(path, args).unwrap()
    });
    if let Some(saved) = saved {
        assert_eq!(&Snapshot::read(&db), saved, "reopening lost durable state or rollback history");
    }

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
    let observed = Arc::new(PeerRequests::default());
    let mut tasks = JoinSet::new();
    tasks.spawn(network);
    tasks.spawn(serve(requests, blocks.clone(), observed.clone()));

    let mut config = super::super::isolated_network_config(chain, directory)
        .with_config(directory.path().join("follower.toml"));
    config.rpc.http = true;
    config.rpc.http_api = Some("eth".parse().unwrap());
    let mut handle = NodeBuilder::new(config)
        .with_database(db.clone())
        .with_launch_context(reth_tasks::Runtime::test())
        .node(EthereumNode::default())
        .launch()
        .await
        .unwrap();
    let client = handle.node.rpc_server_handle().http_client().unwrap();
    let read_checkpoint = || db.tx().unwrap().get::<tables::PartialStateCheckpoints>(0).unwrap();
    let filter = ConfiguredContractFilter::new([TRACKED]);
    let before = Snapshot::read(&db);
    if let Some(saved) = saved {
        assert_eq!(&before, saved, "relaunch must not reset the saved checkpoint");
        assert_eq!(read_checkpoint().unwrap().block_hash, blocks[old_tip].header.hash_slow());
        timeout(WAIT, assert_rpc_state(&client, &blocks[old_tip])).await.unwrap();
        assert_eq!(Snapshot::read(&db), before, "reads before recovery must not modify state");
    } else {
        let tx = db.tx().unwrap();
        assert_eq!(rows::<tables::HashedAccounts>(&tx).len(), 2);
        assert!(!rows::<tables::HashedStorages>(&tx).is_empty());
        assert!(rows::<tables::StageCheckpoints>(&tx)
            .iter()
            .all(|(_, checkpoint)| checkpoint.block_number == 0));
    }
    handle.node.network.add_peer(*peer.peer_id(), peer.local_addr());
    if !restarted {
        wait_for(
            || read_checkpoint().is_some_and(|cp| cp.sync_complete),
            "follower did not bootstrap",
        )
        .await;
        assert_eq!(read_checkpoint().unwrap().block_hash, blocks[0].header.hash_slow());
    }

    // B2 becomes the selected target while the follower is offline on branch A.
    let targets: &[usize] = if !restarted {
        std::slice::from_ref(&old_tip)
    } else if insufficient_history {
        &[4]
    } else {
        &[4, 5]
    };
    for &index in targets {
        let target = &blocks[index];
        let response = timeout(
            WAIT,
            handle.node.add_ons_handle.beacon_engine_handle.fork_choice_updated(
                ForkchoiceState {
                    head_block_hash: target.header.hash_slow(),
                    ..Default::default()
                },
                Some(PayloadAttributes::default()),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.payload_status.status, PayloadStatusEnum::Syncing);
        assert!(response.payload_status.latest_valid_hash.is_none());
        assert!(response.payload_id.is_none());

        let expected = if insufficient_history {
            let err = timeout(WAIT, &mut handle.node_exit_future).await.unwrap().unwrap_err();
            assert!(err.to_string().contains("partial-state follower worker stopped"), "{err}");
            // The retained rollbacks are durable; the pruned A1 journal prevents reaching the seed.
            // The last verified ancestor, not the requested B2 state, must remain readable.
            &blocks[1]
        } else {
            wait_for(
                || read_checkpoint().is_some_and(|cp| cp.block_hash == target.header.hash_slow()),
                "follower did not recover the target checkpoint",
            )
            .await;
            target
        };
        if !restarted && pruned_history {
            wait_for(
                || {
                    db.tx()
                        .unwrap()
                        .get::<tables::PartialStateTransitionJournals>(2)
                        .unwrap()
                        .is_none()
                },
                "old journal was not pruned before shutdown",
            )
            .await;
        }
        let checkpoint = read_checkpoint().unwrap();
        assert!(checkpoint.sync_complete);
        assert_eq!(checkpoint.filter_hash, filter.filter_hash());
        assert_eq!(checkpoint.block_number, expected.header.number);
        assert_eq!(checkpoint.block_hash, expected.header.hash_slow());
        assert_eq!(checkpoint.state_root, expected.header.state_root);
        assert_eq!(
            handle.node.provider.partial_state_root(&filter).unwrap(),
            expected.header.state_root
        );
        let before_rpc = Snapshot::read(&db);
        timeout(WAIT, assert_rpc_state(&client, expected)).await.unwrap();
        if restarted {
            let rejected = if insufficient_history { target } else { &blocks[old_tip] };
            timeout(WAIT, assert_unavailable_checkpoint(&client, rejected.header.hash_slow()))
                .await
                .unwrap();
        }
        assert_eq!(Snapshot::read(&db), before_rpc, "RPC changed persisted state");
        assert_eq!(
            before_rpc.full, before.full,
            "normal full state or execution checkpoints changed"
        );

        let tx = db.tx().unwrap();
        assert_eq!(
            tx.get::<tables::PartialStateHeaders>(expected.header.number).unwrap(),
            Some(expected.header.clone())
        );
        assert_eq!(
            tx.get::<tables::PartialStateStorageRoots>(keccak256(UNTRACKED)).unwrap(),
            Some(expected.accounts()[1].1.storage_root)
        );
        assert!(rows::<tables::PartialStateStorages>(&tx)
            .iter()
            .all(|(address, _)| *address == keccak256(TRACKED)));
        assert!(tx.get::<tables::Bytecodes>(keccak256(UNTRACKED_CODE)).unwrap().is_none());
        if insufficient_history {
            assert!(
                observed.bals.lock().unwrap().is_empty(),
                "must stop before replacement replay"
            );
        } else {
            assert_eq!(
                tx.get::<tables::PartialStateTransitionJournals>(expected.header.number)
                    .unwrap()
                    .unwrap()
                    .block_hash,
                expected.header.hash_slow()
            );
        }
    }

    if restarted {
        assert_eq!(
            observed.snap_downloads.load(Ordering::Relaxed),
            0,
            "restart repeated snap bootstrap"
        );
        if !insufficient_history {
            assert!(observed.account_proofs.load(Ordering::Relaxed) > 0);
            let fetched = observed.bals.lock().unwrap();
            for block in &blocks[3..6] {
                assert!(
                    fetched.contains(&block.header.hash_slow()),
                    "new runtime must fetch replacement BALs"
                );
            }
        }
    } else {
        assert!(observed.snap_downloads.load(Ordering::Relaxed) > 0);
        let tx = db.tx().unwrap();
        assert_eq!(
            tx.get::<tables::PartialStateTransitionJournals>(2).unwrap().is_some(),
            !pruned_history
        );
        assert!(tx
            .get::<tables::PartialStateTransitionJournals>(blocks[old_tip].header.number)
            .unwrap()
            .is_some());
        if pruned_history {
            assert_eq!(
                tx.entries::<tables::PartialStateTransitionJournals>().unwrap(),
                RETENTION as usize
            );
        }
    }
    let result = Snapshot::read(&db);
    if !insufficient_history {
        timeout(WAIT, handle.node.add_ons_handle.engine_shutdown.shutdown().unwrap())
            .await
            .unwrap()
            .unwrap();
    }
    let shutdown = handle.node.task_executor.initiate_graceful_shutdown().unwrap();
    drop(timeout(WAIT, shutdown).await.unwrap());
    if !insufficient_history {
        timeout(WAIT, handle.node_exit_future).await.unwrap().unwrap();
    }
    assert_eq!(Snapshot::read(&db), result, "shutdown changed the verified state");
    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        assert!(result.is_ok() || result.unwrap_err().is_cancelled(), "scripted peer panicked");
    }
    result
}
