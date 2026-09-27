use super::*;
use crate::launch::{
    engine::{
        finish_partial_state_advance, run_partial_state_snap_sync, run_partial_state_snap_sync_once,
    },
    partial_state::bootstrap::{select_trusted_bootstrap, wait_for_bootstrap},
};
use reth_chainspec::EthChainSpec;
use reth_config::PartialStateTrustedCheckpoint;
use reth_storage_api::PartialStateCheckpointStatus;

fn fixture() -> (RecoveryFixture, Arc<Peer>, PartialStateTrustedCheckpoint, RecoveredBlock<Block>) {
    let factory = create_test_provider_factory_with_chain_spec(Arc::new(
        ChainSpecBuilder::mainnet().with_amsterdam_at(0).build(),
    ));
    let header = Header {
        number: 10,
        timestamp: 100,
        state_root: RecoveryFixture::root(10),
        block_access_list_hash: Some(compute_block_access_list_hash(&[])),
        ..Default::default()
    };
    let pivot = PartialStateSnapPivot {
        block_number: header.number,
        block_hash: header.hash_slow(),
        state_root: header.state_root,
    };
    let trusted = PartialStateTrustedCheckpoint {
        chain_id: factory.chain_spec().chain().id(),
        genesis_hash: factory.chain_spec().genesis_hash(),
        block_number: pivot.block_number,
        block_hash: pivot.block_hash,
        state_root: pivot.state_root,
    };
    let fixture = RecoveryFixture { factory, filter: ConfiguredContractFilter::new([]), pivot };
    let child = fixture.block(11, pivot.block_hash, 11);
    let mut peer =
        Peer::new(&[recovered_empty_block(header, pivot.block_hash), child.clone()], &[11]);
    peer.bootstrap_root = Some(pivot.state_root);
    peer.bootstrap_accounts = vec![AccountData {
        hash: keccak256(RecoveryFixture::ADDRESS),
        body: alloy_rlp::encode(RecoveryFixture::account(10)).into(),
    }];
    (fixture, Arc::new(peer), trusted, child)
}

async fn sync(fixture: &RecoveryFixture, peer: Arc<Peer>) {
    run_partial_state_snap_sync(
        peer,
        fixture.factory.clone(),
        fixture.filter.clone(),
        fixture.pivot,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn trusted_bootstrap_downloads_advances_and_resumes_without_local_full_state() {
    let (fixture, peer, trusted, child) = fixture();
    assert_eq!(
        wait_for_bootstrap(
            &peer,
            &fixture.factory,
            &fixture.factory,
            &fixture.filter,
            true,
            Some(trusted)
        )
        .await
        .unwrap(),
        PartialStateBootstrap::Sync(fixture.pivot)
    );
    assert!(fixture.factory.partial_state_checkpoint().unwrap().is_none());
    sync(&fixture, peer.clone()).await;
    fixture.assert_checkpoint(fixture.pivot);

    let mut head = fixture.pivot;
    let (_sender, receiver) = watch::channel(Some(child.hash()));
    let mut advancer = PartialStateAdvancer::new(receiver);
    advance(&mut advancer, &fixture, &peer, &mut head, 64).await.unwrap();
    assert_eq!(head.block_hash, child.hash());
    fixture.assert_checkpoint(head);

    // Resume the later verified checkpoint, not the original seed, without executed headers.
    assert_eq!(
        wait_for_bootstrap(
            &peer,
            &fixture.factory,
            &fixture.factory,
            &fixture.filter,
            true,
            Some(trusted)
        )
        .await
        .unwrap(),
        PartialStateBootstrap::Resume(head)
    );
    assert!(fixture.factory.header(fixture.pivot.block_hash).unwrap().is_none());
    assert!(fixture.factory.header(child.hash()).unwrap().is_none());
    assert_eq!(
        *peer.header_requests.lock().unwrap(),
        [trusted.block_hash, child.hash(), child.hash()]
    );
}

#[tokio::test(start_paused = true)]
async fn trusted_bootstrap_retries_missing_peer_header_without_local_persistence() {
    let (fixture, peer, trusted, _) = fixture();
    peer.header_available.store(false, Ordering::Relaxed);
    let bootstrap = wait_for_bootstrap(
        &peer,
        &fixture.factory,
        &fixture.factory,
        &fixture.filter,
        true,
        Some(trusted),
    );
    tokio::pin!(bootstrap);
    assert!(futures::poll!(&mut bootstrap).is_pending());
    assert!(fixture.factory.partial_state_checkpoint().unwrap().is_none());
    peer.header_available.store(true, Ordering::Relaxed);
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    assert_eq!(bootstrap.await.unwrap(), PartialStateBootstrap::Sync(fixture.pivot));
    assert_eq!(*peer.header_requests.lock().unwrap(), [trusted.block_hash, trusted.block_hash]);
}

#[tokio::test(start_paused = true)]
async fn trusted_bootstrap_preserves_checkpoint_while_snap_root_is_unavailable() {
    let (fixture, peer, _, _) = fixture();
    sync(&fixture, peer.clone()).await;
    let checkpoint = fixture.factory.partial_state_checkpoint().unwrap();
    peer.snap_available.store(false, Ordering::Relaxed);
    let replacement = run_partial_state_snap_sync(
        peer.clone(),
        fixture.factory.clone(),
        fixture.filter.clone(),
        fixture.pivot,
    );
    tokio::pin!(replacement);
    assert!(futures::poll!(&mut replacement).is_pending());
    assert_eq!(fixture.factory.partial_state_checkpoint().unwrap(), checkpoint);
    fixture.assert_checkpoint(fixture.pivot);
    peer.snap_available.store(true, Ordering::Relaxed);
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    replacement.await.unwrap();
    fixture.assert_checkpoint(fixture.pivot);
}

#[tokio::test]
async fn trusted_bootstrap_rejects_wrong_chain_header_height_and_root_without_writes() {
    let (fixture, peer, trusted, _) = fixture();
    sync(&fixture, peer.clone()).await;
    let checkpoint = fixture.factory.partial_state_checkpoint().unwrap();
    for invalid in [
        PartialStateTrustedCheckpoint { chain_id: trusted.chain_id + 1, ..trusted },
        PartialStateTrustedCheckpoint { genesis_hash: B256::repeat_byte(99), ..trusted },
        PartialStateTrustedCheckpoint { block_number: 11, ..trusted },
        PartialStateTrustedCheckpoint { state_root: B256::repeat_byte(99), ..trusted },
    ] {
        assert!(select_trusted_bootstrap(&peer, &fixture.factory, &fixture.filter, false, invalid)
            .await
            .is_err());
        assert_eq!(fixture.factory.partial_state_checkpoint().unwrap(), checkpoint);
        fixture.assert_checkpoint(fixture.pivot);
    }
    *peer.header_override.lock().unwrap() = Some(Header::default());
    assert!(select_trusted_bootstrap(&peer, &fixture.factory, &fixture.filter, false, trusted)
        .await
        .is_err());
    assert_eq!(peer.bad_responses.load(Ordering::Relaxed), 1);
    fixture.assert_checkpoint(fixture.pivot);
}

#[tokio::test]
async fn trusted_bootstrap_rejects_pre_bal_and_missing_bal_commitment() {
    for pre_bal in [false, true] {
        let (mut fixture, _, mut trusted, _) = fixture();
        fixture.factory = create_test_provider_factory_with_chain_spec(Arc::new(
            ChainSpecBuilder::mainnet().with_amsterdam_at(48).build(),
        ));
        let header = Header {
            number: 10,
            timestamp: if pre_bal { 47 } else { 48 },
            state_root: trusted.state_root,
            ..Default::default()
        };
        trusted.block_hash = header.hash_slow();
        trusted.genesis_hash = fixture.factory.chain_spec().genesis_hash();
        let peer = Peer::new(&[recovered_empty_block(header, trusted.block_hash)], &[]);
        assert!(select_trusted_bootstrap(&peer, &fixture.factory, &fixture.filter, false, trusted)
            .await
            .is_err());
        assert!(fixture.factory.partial_state_checkpoint().unwrap().is_none());
    }
}

#[tokio::test]
async fn trusted_bootstrap_does_not_resume_incomplete_or_differently_filtered_state() {
    let (fixture, peer, trusted, _) = fixture();
    fixture.factory.begin_partial_state_sync(fixture.pivot, &fixture.filter).unwrap();
    assert_eq!(
        select_trusted_bootstrap(&peer, &fixture.factory, &fixture.filter, true, trusted)
            .await
            .unwrap(),
        PartialStateBootstrap::Sync(fixture.pivot)
    );
    sync(&fixture, peer.clone()).await;
    let changed = ConfiguredContractFilter::new([Address::repeat_byte(99)]);
    assert_eq!(
        select_trusted_bootstrap(&peer, &fixture.factory, &changed, true, trusted).await.unwrap(),
        PartialStateBootstrap::Sync(fixture.pivot)
    );
    // Selection itself never resets a complete checkpoint.
    fixture.assert_checkpoint(fixture.pivot);
}

#[tokio::test]
async fn trusted_bootstrap_rejects_bad_download_and_corrupt_saved_state() {
    let (fixture, mut peer, trusted, _) = fixture();
    Arc::get_mut(&mut peer).unwrap().bootstrap_accounts[0].body =
        alloy_rlp::encode(RecoveryFixture::account(99)).into();
    assert!(run_partial_state_snap_sync_once(
        peer.clone(),
        fixture.factory.clone(),
        fixture.filter.clone(),
        fixture.pivot
    )
    .await
    .is_err());
    assert_eq!(
        fixture.factory.partial_state_checkpoint().unwrap().unwrap().status,
        PartialStateCheckpointStatus::Syncing
    );
    Arc::get_mut(&mut peer).unwrap().bootstrap_accounts[0].body =
        alloy_rlp::encode(RecoveryFixture::account(10)).into();
    sync(&fixture, peer.clone()).await;
    let provider = fixture.factory.database_provider_rw().unwrap();
    provider
        .partial_state_snap_writer()
        .write_account(keccak256(RecoveryFixture::ADDRESS), RecoveryFixture::account(99))
        .unwrap();
    provider.commit().unwrap();
    assert!(select_trusted_bootstrap(&peer, &fixture.factory, &fixture.filter, true, trusted)
        .await
        .unwrap_err()
        .to_string()
        .contains("checkpoint root mismatch"));
}

#[tokio::test]
async fn trusted_bootstrap_retains_only_tracked_storage_and_code() {
    let (mut fixture, _, mut trusted, _) = fixture();
    let tracked = Address::repeat_byte(3);
    fixture.filter = ConfiguredContractFilter::new([tracked]);
    let code = Bytes::from_static(&[0x60, 0x00]);
    let tracked_account =
        TrieAccount { code_hash: keccak256(&code), ..RecoveryFixture::account(7) };
    let untracked_account =
        TrieAccount { code_hash: keccak256([0x60, 0x01]), ..RecoveryFixture::account(10) };
    let accounts = [(tracked, tracked_account), (RecoveryFixture::ADDRESS, untracked_account)];
    trusted.state_root = state_root_unhashed(accounts);
    let header = Header {
        number: trusted.block_number,
        timestamp: 100,
        state_root: trusted.state_root,
        block_access_list_hash: Some(compute_block_access_list_hash(&[])),
        ..Default::default()
    };
    trusted.block_hash = header.hash_slow();
    fixture.pivot = PartialStateSnapPivot {
        block_number: trusted.block_number,
        block_hash: trusted.block_hash,
        state_root: trusted.state_root,
    };
    let mut peer = Peer::new(&[recovered_empty_block(header, trusted.block_hash)], &[]);
    peer.bootstrap_root = Some(trusted.state_root);
    peer.bootstrap_accounts = accounts
        .into_iter()
        .map(|(addr, account)| AccountData {
            hash: keccak256(addr),
            body: alloy_rlp::encode(account).into(),
        })
        .collect();
    peer.bootstrap_accounts.sort_by_key(|account| account.hash);
    peer.tracked_state = Some((
        keccak256(tracked),
        vec![StorageData {
            hash: keccak256([0u8; 32]),
            data: alloy_rlp::encode(U256::from(7)).into(),
        }],
        code,
    ));
    let peer = Arc::new(peer);
    assert_eq!(
        select_trusted_bootstrap(&peer, &fixture.factory, &fixture.filter, true, trusted)
            .await
            .unwrap(),
        PartialStateBootstrap::Sync(fixture.pivot)
    );
    let progress = run_partial_state_snap_sync(
        peer,
        fixture.factory.clone(),
        fixture.filter.clone(),
        fixture.pivot,
    )
    .await
    .unwrap();
    assert_eq!((progress.accounts, progress.storage_slots, progress.bytecodes), (2, 1, 1));
    assert_eq!((progress.storage_skipped, progress.bytecodes_skipped), (1, 1));
    assert_eq!(fixture.factory.partial_state_root(&fixture.filter).unwrap(), trusted.state_root);
    let provider = fixture.factory.database_provider_ro().unwrap();
    assert_eq!(provider.tx_ref().entries::<tables::PartialStateStorages>().unwrap(), 1);
    assert_eq!(provider.tx_ref().entries::<tables::PlainAccountState>().unwrap(), 0);
    assert_eq!(provider.tx_ref().entries::<tables::PlainStorageState>().unwrap(), 0);
    assert_eq!(provider.tx_ref().entries::<tables::HashedAccounts>().unwrap(), 0);
    assert_eq!(provider.tx_ref().entries::<tables::HashedStorages>().unwrap(), 0);
    assert_eq!(provider.tx_ref().entries::<tables::Bytecodes>().unwrap(), 1);
    assert!(provider
        .tx_ref()
        .get::<tables::Bytecodes>(untracked_account.code_hash)
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn trusted_bootstrap_accepts_verified_empty_state() {
    let (mut fixture, mut peer, _, _) = fixture();
    fixture.pivot.state_root = EMPTY_ROOT_HASH;
    let peer_mut = Arc::get_mut(&mut peer).unwrap();
    peer_mut.bootstrap_accounts.clear();
    peer_mut.bootstrap_root = Some(EMPTY_ROOT_HASH);
    sync(&fixture, peer).await;
    fixture.assert_checkpoint(fixture.pivot);
}

#[tokio::test]
async fn trusted_bootstrap_newer_seed_supersedes_saved_checkpoint_without_resetting_it() {
    let (fixture, peer, mut trusted, child) = fixture();
    sync(&fixture, peer.clone()).await;
    trusted.block_number = child.number();
    trusted.block_hash = child.hash();
    trusted.state_root = child.state_root();
    assert_eq!(
        select_trusted_bootstrap(&peer, &fixture.factory, &fixture.filter, true, trusted)
            .await
            .unwrap(),
        PartialStateBootstrap::Sync(PartialStateSnapPivot {
            block_number: trusted.block_number,
            block_hash: trusted.block_hash,
            state_root: trusted.state_root,
        })
    );
    fixture.assert_checkpoint(fixture.pivot);
}

#[tokio::test]
async fn trusted_bootstrap_recovery_never_falls_back_to_local_persistence() {
    let (fixture, peer, trusted, child) = fixture();
    sync(&fixture, peer.clone()).await;
    let local = BlockchainProvider::with_latest(
        fixture.factory.clone(),
        reth_primitives_traits::SealedHeader::new(child.header().clone(), child.hash()),
    )
    .unwrap();
    let mut head = fixture.pivot;
    for outcome in [
        PartialStateAdvanceOutcome::TargetTooDistant,
        PartialStateAdvanceOutcome::ResyncRequired {
            unavailable_block: child.num_hash(),
            reverted: 0,
        },
        PartialStateAdvanceOutcome::BootstrapRequired { pre_bal_block: child.num_hash() },
    ] {
        let err = finish_partial_state_advance(
            Ok(outcome),
            peer.clone(),
            &fixture.factory,
            &local,
            &fixture.filter,
            &mut head,
            Some(trusted),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("requires a newer trusted checkpoint"));
        assert_eq!(head, fixture.pivot);
        fixture.assert_checkpoint(fixture.pivot);
    }
    assert!(peer.header_requests.lock().unwrap().is_empty());
}
