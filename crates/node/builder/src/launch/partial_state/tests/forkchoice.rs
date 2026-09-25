use super::*;
use reth_eth_wire_types::BlockAccessLists;
use reth_network_p2p::{
    headers::client::{HeadersClient, HeadersFut, HeadersRequest},
    BalRequirement, BlockAccessListsClient,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::watch;

#[derive(Debug)]
struct Peer {
    snap: CommitmentClient,
    headers: BTreeMap<B256, Header>,
    header_available: AtomicBool,
    stall_header: AtomicBool,
    header_override: Mutex<Option<Header>>,
    bals: Mutex<VecDeque<PeerRequestResult<BlockAccessLists>>>,
    header_requests: Mutex<Vec<B256>>,
    bal_requests: AtomicUsize,
    bad_responses: AtomicUsize,
    change_on_proof: Mutex<Option<(watch::Sender<Option<B256>>, B256)>>,
}

impl Peer {
    fn new(blocks: &[RecoveredBlock<Block>], values: &[u64]) -> Self {
        Self {
            snap: RecoveryFixture::client(
                values
                    .iter()
                    .map(|&value| (RecoveryFixture::root(value), RecoveryFixture::response(value)))
                    .collect(),
            ),
            headers: blocks.iter().map(|block| (block.hash(), block.header().clone())).collect(),
            header_available: AtomicBool::new(true),
            stall_header: AtomicBool::new(false),
            header_override: Mutex::new(None),
            bals: Mutex::new(VecDeque::new()),
            header_requests: Mutex::new(Vec::new()),
            bal_requests: AtomicUsize::new(0),
            bad_responses: AtomicUsize::new(0),
            change_on_proof: Mutex::new(None),
        }
    }
}

impl DownloadClient for Peer {
    fn report_bad_message(&self, _: PeerId) {
        self.bad_responses.fetch_add(1, Ordering::Relaxed);
    }
    fn num_connected_peers(&self) -> usize {
        1
    }
}

impl HeadersClient for Peer {
    type Header = Header;
    type Output = HeadersFut;
    fn get_headers_with_priority(
        &self,
        request: HeadersRequest,
        priority: Priority,
    ) -> Self::Output {
        assert_eq!(priority, Priority::High);
        assert_eq!(request.limit, 1);
        let alloy_eips::BlockHashOrNumber::Hash(hash) = request.start else {
            panic!("must request forkchoice ancestry by hash")
        };
        self.header_requests.lock().unwrap().push(hash);
        if self.stall_header.load(Ordering::Relaxed) {
            return Box::pin(future::pending())
        }
        let header = if self.header_available.load(Ordering::Relaxed) {
            self.header_override
                .lock()
                .unwrap()
                .clone()
                .or_else(|| self.headers.get(&hash).cloned())
        } else {
            None
        };
        Box::pin(future::ready(Ok(WithPeerId::new(
            PeerId::repeat_byte(1),
            header.into_iter().collect(),
        ))))
    }
}

impl BlockAccessListsClient for Peer {
    type Output = future::Ready<PeerRequestResult<BlockAccessLists>>;
    fn get_block_access_lists_with_priority_and_requirement(
        &self,
        hashes: Vec<B256>,
        priority: Priority,
        _: BalRequirement,
    ) -> Self::Output {
        assert_eq!(priority, Priority::High);
        assert_eq!(hashes.len(), 1);
        assert!(self.headers.contains_key(&hashes[0]));
        self.bal_requests.fetch_add(1, Ordering::Relaxed);
        future::ready(self.bals.lock().unwrap().pop_front().expect("unexpected BAL request"))
    }
}

impl SnapClient for Peer {
    type Output = NoRequestOutput;
    fn get_account_range_with_priority(
        &self,
        request: GetAccountRangeMessage,
        priority: Priority,
    ) -> Self::Output {
        if let Some((sender, hash)) = self.change_on_proof.lock().unwrap().take() {
            sender.send_replace(Some(hash));
        }
        self.snap.get_account_range_with_priority(request, priority)
    }
    fn get_storage_ranges(&self, _: GetStorageRangesMessage) -> Self::Output {
        panic!("untracked storage")
    }
    fn get_storage_ranges_with_priority(
        &self,
        _: GetStorageRangesMessage,
        _: Priority,
    ) -> Self::Output {
        panic!("untracked storage")
    }
    fn get_byte_codes(&self, _: GetByteCodesMessage) -> Self::Output {
        panic!("untracked code")
    }
    fn get_byte_codes_with_priority(&self, _: GetByteCodesMessage, _: Priority) -> Self::Output {
        panic!("untracked code")
    }
    fn get_trie_nodes(&self, _: GetTrieNodesMessage) -> Self::Output {
        panic!("trie nodes")
    }
    fn get_trie_nodes_with_priority(&self, _: GetTrieNodesMessage, _: Priority) -> Self::Output {
        panic!("trie nodes")
    }
}

async fn advance(
    advancer: &mut PartialStateAdvancer<Header>,
    fixture: &RecoveryFixture,
    peer: &Peer,
    head: &mut PartialStateSnapPivot,
    retention: u64,
) -> eyre::Result<PartialStateAdvanceOutcome> {
    advancer
        .advance(peer, &fixture.factory, &fixture.factory, &fixture.filter, head, retention)
        .await
}

#[tokio::test]
async fn advances_from_forkchoice_while_execution_stays_at_genesis() {
    let fixture = RecoveryFixture::new();
    let mut head = fixture.pivot;
    let first = fixture.block(1, head.block_hash, 11);
    let second = fixture.block(2, first.hash(), 12);
    let peer = Peer::new(&[first.clone(), second.clone()], &[11, 12]);
    let (_sender, receiver) = watch::channel(Some(second.hash()));
    let mut advancer = PartialStateAdvancer::new(receiver);
    assert_eq!(
        advance(&mut advancer, &fixture, &peer, &mut head, 64).await.unwrap(),
        PartialStateAdvanceOutcome::Reconciled { advanced: 2, reverted: 0, pruned: 0 }
    );
    assert_eq!(head.block_hash, second.hash());
    fixture.assert_checkpoint(head);
    assert_eq!(fixture.factory.best_block_number().unwrap(), 0);
    assert!(fixture.factory.header(second.hash()).unwrap().is_none());
    assert_eq!(*peer.header_requests.lock().unwrap(), [second.hash(), first.hash()]);
    assert_eq!(peer.bal_requests.load(Ordering::Relaxed), 0);
}

fn uncached_block(fixture: &RecoveryFixture) -> (RecoveredBlock<Block>, Bytes) {
    let block = fixture.block(1, fixture.pivot.block_hash, 11);
    let raw = fixture.factory.bal_store().get_by_hash(block.hash()).unwrap().unwrap();
    let header = Header { extra_data: Bytes::from_static(b"uncached"), ..block.header().clone() };
    let hash = header.hash_slow();
    (recovered_empty_block(header, hash), raw)
}

#[tokio::test]
async fn downloads_missing_bal_and_verifies_its_header_commitment() {
    let fixture = RecoveryFixture::new();
    let mut head = fixture.pivot;
    let (block, raw) = uncached_block(&fixture);
    let peer = Peer::new(std::slice::from_ref(&block), &[11]);
    peer.bals.lock().unwrap().push_back(Ok(WithPeerId::new(
        PeerId::repeat_byte(1),
        BlockAccessLists(vec![Some(raw.clone())]),
    )));
    let (_sender, receiver) = watch::channel(Some(block.hash()));
    let mut advancer = PartialStateAdvancer::new(receiver);
    advance(&mut advancer, &fixture, &peer, &mut head, 64).await.unwrap();
    fixture.assert_checkpoint(head);
    assert_eq!(head.block_hash, block.hash());
    assert_eq!(peer.bal_requests.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.factory.bal_store().get_by_hash(block.hash()).unwrap(), Some(raw));
    assert_eq!(fixture.factory.best_block_number().unwrap(), 0);
}

#[tokio::test(start_paused = true)]
async fn retries_missing_header_and_bal_without_another_forkchoice() {
    let fixture = RecoveryFixture::new();
    let mut head = fixture.pivot;
    let (block, raw) = uncached_block(&fixture);
    let peer = Peer::new(std::slice::from_ref(&block), &[11]);
    peer.header_available.store(false, Ordering::Relaxed);
    peer.bals.lock().unwrap().extend([
        Err(RequestError::UnsupportedCapability),
        Ok(WithPeerId::new(PeerId::repeat_byte(1), BlockAccessLists(vec![None]))),
        Ok(WithPeerId::new(PeerId::repeat_byte(1), BlockAccessLists(vec![Some(raw)]))),
    ]);
    let (_sender, receiver) = watch::channel(Some(block.hash()));
    let mut advancer = PartialStateAdvancer::new(receiver);
    for _ in 0..3 {
        assert_eq!(
            advance(&mut advancer, &fixture, &peer, &mut head, 64).await.unwrap(),
            PartialStateAdvanceOutcome::AwaitingCommitment
        );
        assert_eq!(head, fixture.pivot);
        fixture.assert_checkpoint(head);
        peer.header_available.store(true, Ordering::Relaxed);
    }
    advance(&mut advancer, &fixture, &peer, &mut head, 64).await.unwrap();
    assert_eq!(head.block_hash, block.hash());
    fixture.assert_checkpoint(head);
}

#[tokio::test(start_paused = true)]
async fn times_out_missing_header_and_resumes_with_same_target() {
    let fixture = RecoveryFixture::new();
    let mut head = fixture.pivot;
    let block = fixture.block(1, head.block_hash, 11);
    let peer = Peer::new(std::slice::from_ref(&block), &[11]);
    peer.stall_header.store(true, Ordering::Relaxed);
    let (_sender, receiver) = watch::channel(Some(block.hash()));
    let mut advancer = PartialStateAdvancer::new(receiver);
    assert_eq!(
        advance(&mut advancer, &fixture, &peer, &mut head, 64).await.unwrap(),
        PartialStateAdvanceOutcome::AwaitingCommitment
    );
    fixture.assert_checkpoint(head);
    peer.stall_header.store(false, Ordering::Relaxed);
    advance(&mut advancer, &fixture, &peer, &mut head, 64).await.unwrap();
    assert_eq!(head.block_hash, block.hash());
}

#[tokio::test]
async fn rejects_wrong_header_and_bal_without_changing_checkpoint() {
    let fixture = RecoveryFixture::new();
    let mut head = fixture.pivot;
    let (block, _) = uncached_block(&fixture);
    let peer = Peer::new(std::slice::from_ref(&block), &[]);
    *peer.header_override.lock().unwrap() = Some(Header::default());
    let (_sender, receiver) = watch::channel(Some(block.hash()));
    let mut advancer = PartialStateAdvancer::new(receiver);
    assert!(advance(&mut advancer, &fixture, &peer, &mut head, 64)
        .await
        .unwrap_err()
        .to_string()
        .contains("header hash mismatch"));
    fixture.assert_checkpoint(head);
    *peer.header_override.lock().unwrap() = None;
    peer.bals.lock().unwrap().push_back(Ok(WithPeerId::new(
        PeerId::repeat_byte(1),
        BlockAccessLists(vec![Some(Bytes::from_static(&[0xc0]))]),
    )));
    assert!(advance(&mut advancer, &fixture, &peer, &mut head, 64).await.is_err());
    assert_eq!(peer.bad_responses.load(Ordering::Relaxed), 2);
    assert!(fixture.factory.bal_store().get_by_hash(block.hash()).unwrap().is_none());
    assert_eq!(head, fixture.pivot);
    fixture.assert_checkpoint(head);
}

#[tokio::test]
async fn discards_proof_when_forkchoice_changes_before_commit() {
    let fixture = RecoveryFixture::new();
    let mut head = fixture.pivot;
    let old = fixture.block(1, head.block_hash, 11);
    let new = fixture.block(1, head.block_hash, 21);
    let peer = Peer::new(&[old.clone(), new.clone()], &[11, 21]);
    let (sender, receiver) = watch::channel(Some(old.hash()));
    *peer.change_on_proof.lock().unwrap() = Some((sender.clone(), new.hash()));
    let mut advancer = PartialStateAdvancer::new(receiver);
    assert_eq!(
        advance(&mut advancer, &fixture, &peer, &mut head, 64).await.unwrap(),
        PartialStateAdvanceOutcome::Reconciled { advanced: 1, reverted: 0, pruned: 0 }
    );
    assert_eq!(head.block_hash, new.hash());
    fixture.assert_checkpoint(head);
    assert_eq!(
        fixture
            .factory
            .database_provider_ro()
            .unwrap()
            .tx_ref()
            .entries::<tables::PartialStateTransitionJournals>()
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn replays_reorg_and_rewind_without_execution_notifications() {
    let fixture = RecoveryFixture::new();
    let mut head = fixture.pivot;
    let old = fixture.block(1, head.block_hash, 11);
    let replacement = fixture.block(1, head.block_hash, 21);
    let second = fixture.block(2, replacement.hash(), 22);
    let peer = Peer::new(&[old.clone(), replacement.clone(), second.clone()], &[11, 21, 22]);
    let (sender, receiver) = watch::channel(Some(old.hash()));
    let mut advancer = PartialStateAdvancer::new(receiver);
    advance(&mut advancer, &fixture, &peer, &mut head, 64).await.unwrap();
    sender.send_replace(Some(second.hash()));
    assert_eq!(
        advance(&mut advancer, &fixture, &peer, &mut head, 64).await.unwrap(),
        PartialStateAdvanceOutcome::Reconciled { advanced: 2, reverted: 1, pruned: 0 }
    );
    assert_eq!(head.block_hash, second.hash());
    fixture.assert_checkpoint(head);
    sender.send_replace(Some(replacement.hash()));
    assert_eq!(
        advance(&mut advancer, &fixture, &peer, &mut head, 64).await.unwrap(),
        PartialStateAdvanceOutcome::Reconciled { advanced: 0, reverted: 1, pruned: 0 }
    );
    assert_eq!(head.block_hash, replacement.hash());
    fixture.assert_checkpoint(head);
    assert_eq!(fixture.factory.best_block_number().unwrap(), 0);
}

#[tokio::test]
async fn coalesces_targets_and_stops_when_forkchoice_stream_closes() {
    let fixture = RecoveryFixture::new();
    let mut head = fixture.pivot;
    let block = fixture.block(1, head.block_hash, 11);
    let peer = Peer::new(std::slice::from_ref(&block), &[11]);
    let (sender, receiver) = watch::channel(None);
    let mut advancer = PartialStateAdvancer::new(receiver);
    sender.send_replace(Some(B256::repeat_byte(9)));
    sender.send_replace(Some(block.hash()));
    advance(&mut advancer, &fixture, &peer, &mut head, 64).await.unwrap();
    assert_eq!(*peer.header_requests.lock().unwrap(), [block.hash()]);
    drop(sender);
    assert!(advance(&mut advancer, &fixture, &peer, &mut head, 64).await.is_err());
    fixture.assert_checkpoint(head);
}

#[tokio::test(start_paused = true)]
async fn retries_peer_proofs_without_new_forkchoice_but_rejects_invalid_proofs() {
    for valid in [true, false] {
        let fixture = RecoveryFixture::new();
        let mut head = fixture.pivot;
        let block = fixture.block(1, head.block_hash, 11);
        let peer = Peer::new(std::slice::from_ref(&block), &[]);
        peer.snap.responses.lock().unwrap().extend([
            (block.state_root(), Err(RequestError::UnsupportedCapability)),
            (block.state_root(), RecoveryFixture::response(if valid { 11 } else { 10 })),
        ]);
        let (_sender, receiver) = watch::channel(Some(block.hash()));
        let mut advancer = PartialStateAdvancer::new(receiver);
        assert_eq!(
            advance(&mut advancer, &fixture, &peer, &mut head, 64).await.unwrap(),
            PartialStateAdvanceOutcome::AwaitingCommitment
        );
        assert_eq!(head, fixture.pivot);
        fixture.assert_checkpoint(head);
        let result = advance(&mut advancer, &fixture, &peer, &mut head, 64).await;
        if valid {
            result.unwrap();
            assert_eq!(head.block_hash, block.hash());
        } else {
            assert!(matches!(
                result.unwrap_err().downcast_ref::<PartialStateAccountResolverError>(),
                Some(PartialStateAccountResolverError::InvalidProof { .. })
            ));
            assert_eq!(head, fixture.pivot);
        }
        fixture.assert_checkpoint(head);
    }
}

#[tokio::test]
async fn requests_resync_when_forkchoice_reorg_exceeds_retention() {
    let fixture = RecoveryFixture::new();
    let mut head = fixture.pivot;
    let first = fixture.block(1, head.block_hash, 11);
    let second = fixture.block(2, first.hash(), 12);
    let replacement = fixture.block(1, fixture.pivot.block_hash, 21);
    let peer = Peer::new(&[first, second.clone(), replacement.clone()], &[11, 12]);
    let (sender, receiver) = watch::channel(Some(second.hash()));
    let mut advancer = PartialStateAdvancer::new(receiver);
    assert_eq!(
        advance(&mut advancer, &fixture, &peer, &mut head, 1).await.unwrap(),
        PartialStateAdvanceOutcome::Reconciled { advanced: 2, reverted: 0, pruned: 1 }
    );
    sender.send_replace(Some(replacement.hash()));
    assert!(matches!(
        advance(&mut advancer, &fixture, &peer, &mut head, 1).await.unwrap(),
        PartialStateAdvanceOutcome::ResyncRequired { reverted: 1, .. }
    ));
    assert_eq!(head.block_number, 1);
    fixture.assert_checkpoint(head);
}

#[tokio::test]
async fn rejects_inconsistent_parent_number_before_replay() {
    let fixture = RecoveryFixture::new();
    let mut head = fixture.pivot;
    let block = fixture.block(2, head.block_hash, 11);
    let peer = Peer::new(std::slice::from_ref(&block), &[]);
    let (_sender, receiver) = watch::channel(Some(block.hash()));
    let mut advancer = PartialStateAdvancer::new(receiver);
    assert!(advance(&mut advancer, &fixture, &peer, &mut head, 64)
        .await
        .unwrap_err()
        .to_string()
        .contains("inconsistent parent number"));
    assert_eq!(head, fixture.pivot);
    fixture.assert_checkpoint(head);
}

#[tokio::test]
async fn distant_target_preserves_checkpoint_and_bounds_downloads() {
    let fixture = RecoveryFixture::new();
    let mut head = fixture.pivot;
    let block = fixture.block(4097, B256::repeat_byte(9), 11);
    let peer = Peer::new(std::slice::from_ref(&block), &[]);
    let (_sender, receiver) = watch::channel(Some(block.hash()));
    let mut advancer = PartialStateAdvancer::new(receiver);
    assert_eq!(
        advance(&mut advancer, &fixture, &peer, &mut head, 64).await.unwrap(),
        PartialStateAdvanceOutcome::TargetTooDistant
    );
    assert_eq!(head, fixture.pivot);
    assert_eq!(peer.header_requests.lock().unwrap().len(), 1);
    fixture.assert_checkpoint(head);
}

#[tokio::test(start_paused = true)]
async fn cancels_inflight_header_on_new_forkchoice_without_waiting_for_timeout() {
    let fixture = RecoveryFixture::new();
    let mut head = fixture.pivot;
    let old = fixture.block(1, head.block_hash, 11);
    let new = fixture.block(1, head.block_hash, 21);
    let peer = Peer::new(&[old.clone(), new.clone()], &[21]);
    peer.stall_header.store(true, Ordering::Relaxed);
    let (sender, receiver) = watch::channel(Some(old.hash()));
    let mut advancer = PartialStateAdvancer::new(receiver);
    let work = advance(&mut advancer, &fixture, &peer, &mut head, 64);
    tokio::pin!(work);
    tokio::select! {
        biased;
        result = &mut work => panic!("expected a pending header request: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    peer.stall_header.store(false, Ordering::Relaxed);
    sender.send_replace(Some(new.hash()));
    tokio::time::timeout(Duration::from_secs(2), work).await.unwrap().unwrap();
    // The future owns the mutable head reference until it is dropped at the end of its scope.
    assert_eq!(
        fixture.factory.partial_state_checkpoint().unwrap().unwrap().pivot.block_hash,
        new.hash()
    );
    assert_eq!(*peer.header_requests.lock().unwrap(), [old.hash(), new.hash()]);
}
