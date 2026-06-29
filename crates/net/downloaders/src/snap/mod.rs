//! Partial-state snap downloader scaffolding.

use alloy_primitives::B256;
use futures::{Future, Stream};
use futures_util::FutureExt;
use reth_eth_wire_types::snap::{AccountRangeMessage, GetAccountRangeMessage};
use reth_network_p2p::{
    error::{PeerRequestResult, RequestError},
    priority::Priority,
    snap::client::{SnapClient, SnapResponse},
};
use reth_network_peers::PeerId;
use std::{
    pin::Pin,
    task::{ready, Context, Poll},
};
use thiserror::Error;

/// Default soft byte limit for partial-state snap requests.
pub const DEFAULT_PARTIAL_STATE_SNAP_RESPONSE_BYTES: u64 = 2 * 1024 * 1024;

/// Snap state range to download.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartialStateSnapTarget {
    /// Root hash of the account trie to download.
    pub root_hash: B256,
    /// First account hash to request.
    pub starting_hash: B256,
    /// Account hash after which peers should stop serving data.
    pub limit_hash: B256,
}

impl PartialStateSnapTarget {
    /// Creates a target that covers the full account hash range for the given state root.
    pub const fn full_range(root_hash: B256) -> Self {
        Self { root_hash, starting_hash: B256::ZERO, limit_hash: B256::repeat_byte(0xff) }
    }
}

/// Configuration for the partial-state snap downloader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartialStateSnapDownloaderConfig {
    /// Soft byte limit passed to snap range requests.
    pub response_bytes: u64,
}

impl Default for PartialStateSnapDownloaderConfig {
    fn default() -> Self {
        Self { response_bytes: DEFAULT_PARTIAL_STATE_SNAP_RESPONSE_BYTES }
    }
}

/// Progress counters tracked by the partial-state snap downloader.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PartialStateSnapProgress {
    /// Number of account leaves returned by peers.
    pub accounts: u64,
    /// Number of account proof nodes returned by peers.
    pub account_proofs: u64,
    /// Number of account-range requests completed successfully.
    pub account_range_responses: u64,
}

/// Stream item emitted by the partial-state snap downloader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartialStateSnapEvent {
    /// Account range data returned by a snap peer.
    AccountRange {
        /// Peer that served the response.
        peer_id: PeerId,
        /// Account range response.
        response: AccountRangeMessage,
        /// Progress after applying this response.
        progress: PartialStateSnapProgress,
    },
}

/// Error returned by the partial-state snap downloader.
#[derive(Debug, Error)]
pub enum PartialStateSnapDownloaderError {
    /// The downloader was polled before a target was configured.
    #[error("partial-state snap downloader has no target")]
    MissingTarget,
    /// The account hash range is exhausted.
    #[error("partial-state snap account hash range is exhausted")]
    AccountRangeExhausted,
    /// Snap request failed.
    #[error(transparent)]
    Request(#[from] RequestError),
    /// Peer returned a snap response that does not match the active request.
    #[error("unexpected snap response: {0}")]
    UnexpectedResponse(&'static str),
}

/// Downloads snap account ranges for a partial-state initial sync.
///
/// This skeleton deliberately starts with account-range orchestration. The follow-up stages can use
/// the emitted account hashes to derive filtered storage and bytecode requests while keeping all
/// account leaves available locally.
#[must_use = "Stream does nothing unless polled"]
#[derive(Debug)]
pub struct PartialStateSnapDownloader<C: SnapClient> {
    /// Client used to send snap requests.
    client: C,
    /// Downloader configuration.
    config: PartialStateSnapDownloaderConfig,
    /// Active account range target.
    target: Option<PartialStateSnapTarget>,
    /// Next account hash to request.
    next_account_hash: B256,
    /// Account-range request in flight.
    in_flight_account_range: Option<AccountRangeRequestFuture<C::Output>>,
    /// Progress counters.
    progress: PartialStateSnapProgress,
    /// Whether the configured target has completed.
    finished: bool,
}

impl<C> PartialStateSnapDownloader<C>
where
    C: SnapClient,
{
    /// Creates a new partial-state snap downloader.
    pub fn new(client: C) -> Self {
        Self::with_config(client, PartialStateSnapDownloaderConfig::default())
    }

    /// Creates a new partial-state snap downloader with the given configuration.
    pub const fn with_config(client: C, config: PartialStateSnapDownloaderConfig) -> Self {
        Self {
            client,
            config,
            target: None,
            next_account_hash: B256::ZERO,
            in_flight_account_range: None,
            progress: PartialStateSnapProgress {
                accounts: 0,
                account_proofs: 0,
                account_range_responses: 0,
            },
            finished: false,
        }
    }

    /// Starts downloading the given snap state target.
    pub fn start(&mut self, target: PartialStateSnapTarget) {
        self.target = Some(target);
        self.next_account_hash = target.starting_hash;
        self.in_flight_account_range = None;
        self.progress =
            PartialStateSnapProgress { accounts: 0, account_proofs: 0, account_range_responses: 0 };
        self.finished = false;
    }

    /// Returns the active target.
    pub const fn target(&self) -> Option<PartialStateSnapTarget> {
        self.target
    }

    /// Returns the current progress counters.
    pub const fn progress(&self) -> PartialStateSnapProgress {
        self.progress
    }

    /// Returns true if the active target has completed.
    pub const fn is_finished(&self) -> bool {
        self.finished
    }

    /// Queues the next account-range request if no request is currently in flight.
    fn queue_next_account_range(&mut self) -> Result<bool, PartialStateSnapDownloaderError> {
        if self.finished || self.in_flight_account_range.is_some() {
            return Ok(false)
        }

        let target = self.target.ok_or(PartialStateSnapDownloaderError::MissingTarget)?;
        if self.next_account_hash > target.limit_hash {
            self.finished = true;
            return Ok(false)
        }

        let request = GetAccountRangeMessage {
            request_id: 0,
            root_hash: target.root_hash,
            starting_hash: self.next_account_hash,
            limit_hash: target.limit_hash,
            response_bytes: self.config.response_bytes,
        };
        let fut = self.client.get_account_range_with_priority(request.clone(), Priority::High);
        self.in_flight_account_range = Some(AccountRangeRequestFuture { request, fut });
        Ok(true)
    }

    /// Handles an account-range response and advances the account cursor.
    fn on_account_range_response(
        &mut self,
        peer_id: PeerId,
        response: AccountRangeMessage,
    ) -> Result<PartialStateSnapEvent, PartialStateSnapDownloaderError> {
        self.progress.accounts += response.accounts.len() as u64;
        self.progress.account_proofs += response.proof.len() as u64;
        self.progress.account_range_responses += 1;

        let Some(last_account) = response.accounts.last() else {
            self.finished = true;
            return Ok(PartialStateSnapEvent::AccountRange {
                peer_id,
                response,
                progress: self.progress,
            })
        };

        let target = self.target.ok_or(PartialStateSnapDownloaderError::MissingTarget)?;
        if last_account.hash >= target.limit_hash {
            self.finished = true;
        } else {
            self.next_account_hash = next_hash(last_account.hash)
                .ok_or(PartialStateSnapDownloaderError::AccountRangeExhausted)?;
        }

        Ok(PartialStateSnapEvent::AccountRange { peer_id, response, progress: self.progress })
    }
}

impl<C> Stream for PartialStateSnapDownloader<C>
where
    C: SnapClient + Unpin,
{
    type Item = Result<PartialStateSnapEvent, PartialStateSnapDownloaderError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.finished {
            return Poll::Ready(None)
        }

        if let Err(error) = this.queue_next_account_range() {
            return Poll::Ready(Some(Err(error)))
        }

        if this.finished {
            return Poll::Ready(None)
        }

        let Some(mut request) = this.in_flight_account_range.take() else { return Poll::Pending };

        match request.poll_unpin(cx) {
            Poll::Ready(AccountRangeRequestOutcome { outcome, .. }) => match outcome {
                Ok(response) => {
                    let (peer_id, response) = response.split();
                    match response {
                        SnapResponse::AccountRange(response) => {
                            Poll::Ready(Some(this.on_account_range_response(peer_id, response)))
                        }
                        SnapResponse::StorageRanges(_) => Poll::Ready(Some(Err(
                            PartialStateSnapDownloaderError::UnexpectedResponse("storage ranges"),
                        ))),
                        SnapResponse::ByteCodes(_) => Poll::Ready(Some(Err(
                            PartialStateSnapDownloaderError::UnexpectedResponse("bytecodes"),
                        ))),
                        SnapResponse::TrieNodes(_) => Poll::Ready(Some(Err(
                            PartialStateSnapDownloaderError::UnexpectedResponse("trie nodes"),
                        ))),
                    }
                }
                Err(error) => Poll::Ready(Some(Err(error.into()))),
            },
            Poll::Pending => {
                this.in_flight_account_range = Some(request);
                Poll::Pending
            }
        }
    }
}

/// Future returned for an account-range request.
#[derive(Debug)]
struct AccountRangeRequestFuture<F> {
    request: GetAccountRangeMessage,
    fut: F,
}

impl<F> Future for AccountRangeRequestFuture<F>
where
    F: Future<Output = PeerRequestResult<SnapResponse>> + Send + Sync + Unpin,
{
    type Output = AccountRangeRequestOutcome;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let outcome = ready!(this.fut.poll_unpin(cx));

        Poll::Ready(AccountRangeRequestOutcome { request: this.request.clone(), outcome })
    }
}

/// Account-range request outcome.
#[derive(Debug)]
struct AccountRangeRequestOutcome {
    #[allow(dead_code)]
    request: GetAccountRangeMessage,
    outcome: PeerRequestResult<SnapResponse>,
}

/// Returns the next account hash after `hash`.
fn next_hash(mut hash: B256) -> Option<B256> {
    for byte in hash.as_mut_slice().iter_mut().rev() {
        let (next, overflow) = byte.overflowing_add(1);
        *byte = next;
        if !overflow {
            return Some(hash)
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::Bytes;
    use futures_util::StreamExt;
    use reth_eth_wire_types::snap::{
        ByteCodesMessage, GetByteCodesMessage, GetStorageRangesMessage, GetTrieNodesMessage,
        StorageRangesMessage, TrieNodesMessage,
    };
    use reth_network_p2p::download::DownloadClient;
    use reth_network_peers::WithPeerId;
    use std::{
        collections::VecDeque,
        fmt,
        sync::{Arc, Mutex},
    };

    #[tokio::test]
    async fn requests_account_range_for_target() {
        let root = B256::repeat_byte(0x11);
        let peer = PeerId::repeat_byte(0x22);
        let response = AccountRangeMessage {
            request_id: 0,
            accounts: vec![],
            proof: vec![Bytes::from_static(&[0x01])],
        };
        let client = MockSnapClient::new([Ok(WithPeerId::new(
            peer,
            SnapResponse::AccountRange(response.clone()),
        ))]);
        let mut downloader = PartialStateSnapDownloader::new(client.clone());

        downloader.start(PartialStateSnapTarget::full_range(root));

        let event = downloader.next().await.unwrap().unwrap();
        assert_eq!(
            client.account_range_requests(),
            vec![GetAccountRangeMessage {
                request_id: 0,
                root_hash: root,
                starting_hash: B256::ZERO,
                limit_hash: B256::repeat_byte(0xff),
                response_bytes: DEFAULT_PARTIAL_STATE_SNAP_RESPONSE_BYTES,
            }]
        );
        assert_eq!(
            event,
            PartialStateSnapEvent::AccountRange {
                peer_id: peer,
                response,
                progress: PartialStateSnapProgress {
                    accounts: 0,
                    account_proofs: 1,
                    account_range_responses: 1,
                },
            }
        );
        assert!(downloader.is_finished());
    }

    #[tokio::test]
    async fn advances_account_cursor_after_non_empty_response() {
        let root = B256::repeat_byte(0x11);
        let peer = PeerId::repeat_byte(0x22);
        let first_hash = B256::repeat_byte(0x01);
        let second_hash = B256::repeat_byte(0x02);
        let client = MockSnapClient::new([
            Ok(WithPeerId::new(
                peer,
                SnapResponse::AccountRange(AccountRangeMessage {
                    request_id: 0,
                    accounts: vec![reth_eth_wire_types::snap::AccountData {
                        hash: first_hash,
                        body: Bytes::new(),
                    }],
                    proof: vec![],
                }),
            )),
            Ok(WithPeerId::new(
                peer,
                SnapResponse::AccountRange(AccountRangeMessage {
                    request_id: 0,
                    accounts: vec![reth_eth_wire_types::snap::AccountData {
                        hash: second_hash,
                        body: Bytes::new(),
                    }],
                    proof: vec![],
                }),
            )),
        ]);
        let mut downloader = PartialStateSnapDownloader::with_config(
            client.clone(),
            PartialStateSnapDownloaderConfig { response_bytes: 1024 },
        );

        downloader.start(PartialStateSnapTarget {
            root_hash: root,
            starting_hash: first_hash,
            limit_hash: second_hash,
        });

        assert!(downloader.next().await.unwrap().is_ok());
        assert!(downloader.next().await.unwrap().is_ok());

        let requests = client.account_range_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].starting_hash, first_hash);
        assert_eq!(requests[1].starting_hash, next_hash(first_hash).unwrap());
        assert_eq!(requests[1].response_bytes, 1024);
        assert!(downloader.is_finished());
        assert_eq!(
            downloader.progress(),
            PartialStateSnapProgress { accounts: 2, account_proofs: 0, account_range_responses: 2 }
        );
    }

    #[tokio::test]
    async fn rejects_unexpected_snap_response() {
        let peer = PeerId::repeat_byte(0x22);
        let client = MockSnapClient::new([Ok(WithPeerId::new(
            peer,
            SnapResponse::ByteCodes(ByteCodesMessage { request_id: 0, codes: vec![] }),
        ))]);
        let mut downloader = PartialStateSnapDownloader::new(client);

        downloader.start(PartialStateSnapTarget::full_range(B256::repeat_byte(0x11)));

        let error = downloader.next().await.unwrap().unwrap_err();
        assert!(matches!(error, PartialStateSnapDownloaderError::UnexpectedResponse("bytecodes")));
    }

    #[test]
    fn increments_account_hash() {
        assert_eq!(next_hash(B256::ZERO), Some(B256::with_last_byte(1)));
        assert_eq!(next_hash(B256::repeat_byte(0xff)), None);

        let mut hash = B256::ZERO;
        hash.as_mut_slice()[31] = 0xff;
        let mut expected = B256::ZERO;
        expected.as_mut_slice()[30] = 1;
        assert_eq!(next_hash(hash), Some(expected));
    }

    #[derive(Clone, Default)]
    struct MockSnapClient {
        responses: Arc<Mutex<VecDeque<PeerRequestResult<SnapResponse>>>>,
        account_range_requests: Arc<Mutex<Vec<GetAccountRangeMessage>>>,
    }

    impl MockSnapClient {
        fn new(responses: impl IntoIterator<Item = PeerRequestResult<SnapResponse>>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into_iter().collect())),
                account_range_requests: Default::default(),
            }
        }

        fn next_response(&self) -> MockSnapClientOutput {
            let response =
                self.responses.lock().unwrap().pop_front().unwrap_or(Err(RequestError::Timeout));
            Box::pin(futures::future::ready(response))
        }

        fn account_range_requests(&self) -> Vec<GetAccountRangeMessage> {
            self.account_range_requests.lock().unwrap().clone()
        }
    }

    impl fmt::Debug for MockSnapClient {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("MockSnapClient").finish_non_exhaustive()
        }
    }

    impl DownloadClient for MockSnapClient {
        fn report_bad_message(&self, _peer_id: PeerId) {}

        fn num_connected_peers(&self) -> usize {
            1
        }
    }

    type MockSnapClientOutput =
        Pin<Box<dyn Future<Output = PeerRequestResult<SnapResponse>> + Send + Sync>>;

    impl SnapClient for MockSnapClient {
        type Output = MockSnapClientOutput;

        fn get_account_range_with_priority(
            &self,
            request: GetAccountRangeMessage,
            _priority: Priority,
        ) -> Self::Output {
            self.account_range_requests.lock().unwrap().push(request);
            self.next_response()
        }

        fn get_storage_ranges(&self, request: GetStorageRangesMessage) -> Self::Output {
            self.get_storage_ranges_with_priority(request, Priority::Normal)
        }

        fn get_storage_ranges_with_priority(
            &self,
            _request: GetStorageRangesMessage,
            _priority: Priority,
        ) -> Self::Output {
            Box::pin(futures::future::ready(Ok(WithPeerId::new(
                PeerId::default(),
                SnapResponse::StorageRanges(StorageRangesMessage {
                    request_id: 0,
                    slots: vec![],
                    proof: vec![],
                }),
            ))))
        }

        fn get_byte_codes(&self, request: GetByteCodesMessage) -> Self::Output {
            self.get_byte_codes_with_priority(request, Priority::Normal)
        }

        fn get_byte_codes_with_priority(
            &self,
            _request: GetByteCodesMessage,
            _priority: Priority,
        ) -> Self::Output {
            Box::pin(futures::future::ready(Ok(WithPeerId::new(
                PeerId::default(),
                SnapResponse::ByteCodes(ByteCodesMessage { request_id: 0, codes: vec![] }),
            ))))
        }

        fn get_trie_nodes(&self, request: GetTrieNodesMessage) -> Self::Output {
            self.get_trie_nodes_with_priority(request, Priority::Normal)
        }

        fn get_trie_nodes_with_priority(
            &self,
            _request: GetTrieNodesMessage,
            _priority: Priority,
        ) -> Self::Output {
            Box::pin(futures::future::ready(Ok(WithPeerId::new(
                PeerId::default(),
                SnapResponse::TrieNodes(TrieNodesMessage { request_id: 0, nodes: vec![] }),
            ))))
        }
    }
}
