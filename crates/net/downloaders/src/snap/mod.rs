//! Partial-state snap downloader scaffolding.

use alloy_consensus::constants::{EMPTY_ROOT_HASH, KECCAK_EMPTY};
use alloy_primitives::{B256, U256};
use alloy_rlp::Decodable;
use futures::{Future, Stream};
use futures_util::FutureExt;
use reth_eth_wire_types::snap::{
    AccountRangeMessage, ByteCodesMessage, GetAccountRangeMessage, GetByteCodesMessage,
    GetStorageRangesMessage, StorageRangesMessage,
};
use reth_network_p2p::{
    error::{PeerRequestResult, RequestError},
    priority::Priority,
    snap::client::{SnapClient, SnapResponse},
};
use reth_network_peers::PeerId;
use reth_storage_api::{AllowAllContractFilter, ContractFilter, PartialStateSnapWriter};
use reth_trie_common::TrieAccount;
use std::{
    collections::VecDeque,
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
    /// Number of account leaf bytes returned by peers.
    pub account_bytes: u64,
    /// Number of account proof nodes returned by peers.
    pub account_proofs: u64,
    /// Number of account proof bytes returned by peers.
    pub account_proof_bytes: u64,
    /// Number of storage range requests completed successfully.
    pub storage_range_responses: u64,
    /// Number of storage slots returned by peers.
    pub storage_slots: u64,
    /// Number of storage slot/proof bytes returned by peers.
    pub storage_bytes: u64,
    /// Number of storage tries skipped because the account is untracked.
    pub storage_skipped: u64,
    /// Number of bytecode requests completed successfully.
    pub bytecode_responses: u64,
    /// Number of bytecodes returned by peers.
    pub bytecodes: u64,
    /// Number of bytecode bytes returned by peers.
    pub bytecode_bytes: u64,
    /// Number of bytecodes skipped because the account is untracked.
    pub bytecodes_skipped: u64,
    /// Number of account-range requests completed successfully.
    pub account_range_responses: u64,
}

impl PartialStateSnapProgress {
    /// Returns the total downloaded state payload bytes seen by this downloader.
    pub const fn state_bytes(&self) -> u64 {
        self.account_bytes + self.account_proof_bytes + self.storage_bytes + self.bytecode_bytes
    }
}

/// Stream item emitted by the partial-state snap downloader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartialStateSnapEvent {
    /// Account range data returned by a snap peer.
    AccountRange {
        /// Request that produced this response.
        request: GetAccountRangeMessage,
        /// Peer that served the response.
        peer_id: PeerId,
        /// Account range response.
        response: AccountRangeMessage,
        /// Progress after applying this response.
        progress: PartialStateSnapProgress,
    },
    /// Storage ranges returned by a snap peer.
    StorageRanges {
        /// Request that produced this response.
        request: GetStorageRangesMessage,
        /// Peer that served the response.
        peer_id: PeerId,
        /// Storage ranges response.
        response: StorageRangesMessage,
        /// Progress after applying this response.
        progress: PartialStateSnapProgress,
    },
    /// Bytecodes returned by a snap peer.
    ByteCodes {
        /// Request that produced this response.
        request: GetByteCodesMessage,
        /// Peer that served the response.
        peer_id: PeerId,
        /// Bytecodes response.
        response: ByteCodesMessage,
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
    /// Account leaf body failed to decode.
    #[error("failed to decode snap account {account_hash}")]
    AccountDecode {
        /// Account hash whose body failed to decode.
        account_hash: B256,
        /// RLP decoding error.
        source: alloy_rlp::Error,
    },
    /// Peer returned a snap response that does not match the active request.
    #[error("unexpected snap response: {0}")]
    UnexpectedResponse(&'static str),
}

/// Downloads snap account ranges for a partial-state initial sync.
///
/// The downloader keeps every account leaf visible to the caller and derives filtered storage and
/// bytecode requests from those account leaves. Successful events can be wrapped with
/// [`PersistedPartialStateSnapDownloader`] to write the returned state records before yielding
/// them.
#[must_use = "Stream does nothing unless polled"]
#[derive(Debug)]
pub struct PartialStateSnapDownloader<C: SnapClient, F = AllowAllContractFilter>
where
    F: ContractFilter,
{
    /// Client used to send snap requests.
    client: C,
    /// Optional partial-state filter.
    filter: Option<F>,
    /// Downloader configuration.
    config: PartialStateSnapDownloaderConfig,
    /// Active account range target.
    target: Option<PartialStateSnapTarget>,
    /// Next account hash to request.
    next_account_hash: B256,
    /// Account-range request in flight.
    in_flight_account_range: Option<AccountRangeRequestFuture<C::Output>>,
    /// Storage range requests waiting to be sent.
    pending_storage_ranges: VecDeque<GetStorageRangesMessage>,
    /// Bytecode requests waiting to be sent.
    pending_bytecodes: VecDeque<GetByteCodesMessage>,
    /// Storage range request in flight.
    in_flight_storage_ranges: Option<StorageRangesRequestFuture<C::Output>>,
    /// Bytecode request in flight.
    in_flight_bytecodes: Option<ByteCodesRequestFuture<C::Output>>,
    /// Progress counters.
    progress: PartialStateSnapProgress,
    /// Whether the configured target has completed.
    finished: bool,
}

impl<C> PartialStateSnapDownloader<C, AllowAllContractFilter>
where
    C: SnapClient,
{
    /// Creates a new partial-state snap downloader.
    pub fn new(client: C) -> Self {
        Self::with_config(client, PartialStateSnapDownloaderConfig::default())
    }

    /// Creates a new partial-state snap downloader with the given configuration.
    pub fn with_config(client: C, config: PartialStateSnapDownloaderConfig) -> Self {
        Self {
            client,
            filter: None,
            config,
            target: None,
            next_account_hash: B256::ZERO,
            in_flight_account_range: None,
            pending_storage_ranges: VecDeque::new(),
            pending_bytecodes: VecDeque::new(),
            in_flight_storage_ranges: None,
            in_flight_bytecodes: None,
            progress: PartialStateSnapProgress::new(),
            finished: false,
        }
    }
}

impl<C, F> PartialStateSnapDownloader<C, F>
where
    C: SnapClient,
    F: ContractFilter,
{
    /// Creates a new partial-state snap downloader with a contract filter.
    pub fn with_filter(client: C, config: PartialStateSnapDownloaderConfig, filter: F) -> Self {
        Self {
            client,
            filter: Some(filter),
            config,
            target: None,
            next_account_hash: B256::ZERO,
            in_flight_account_range: None,
            pending_storage_ranges: VecDeque::new(),
            pending_bytecodes: VecDeque::new(),
            in_flight_storage_ranges: None,
            in_flight_bytecodes: None,
            progress: PartialStateSnapProgress::new(),
            finished: false,
        }
    }

    /// Starts downloading the given snap state target.
    pub fn start(&mut self, target: PartialStateSnapTarget) {
        self.target = Some(target);
        self.next_account_hash = target.starting_hash;
        self.in_flight_account_range = None;
        self.pending_storage_ranges.clear();
        self.pending_bytecodes.clear();
        self.in_flight_storage_ranges = None;
        self.in_flight_bytecodes = None;
        self.progress = PartialStateSnapProgress::new();
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
        if self.finished || self.in_flight_account_range.is_some() || self.has_state_work() {
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
        request: GetAccountRangeMessage,
        peer_id: PeerId,
        response: AccountRangeMessage,
    ) -> Result<PartialStateSnapEvent, PartialStateSnapDownloaderError> {
        self.progress.accounts += response.accounts.len() as u64;
        self.progress.account_bytes +=
            response.accounts.iter().map(|account| account.body.len() as u64).sum::<u64>();
        self.progress.account_proofs += response.proof.len() as u64;
        self.progress.account_proof_bytes +=
            response.proof.iter().map(|proof| proof.len() as u64).sum::<u64>();
        self.progress.account_range_responses += 1;

        self.queue_state_requests_from_accounts(&response)?;

        let Some(last_account) = response.accounts.last() else {
            self.finished = true;
            return Ok(PartialStateSnapEvent::AccountRange {
                request,
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

        Ok(PartialStateSnapEvent::AccountRange {
            request,
            peer_id,
            response,
            progress: self.progress,
        })
    }

    /// Handles a storage-ranges response and updates progress counters.
    fn on_storage_ranges_response(
        &mut self,
        request: GetStorageRangesMessage,
        peer_id: PeerId,
        response: StorageRangesMessage,
    ) -> PartialStateSnapEvent {
        self.progress.storage_range_responses += 1;
        self.progress.storage_slots +=
            response.slots.iter().map(|slots| slots.len() as u64).sum::<u64>();
        self.progress.storage_bytes +=
            response.slots.iter().flatten().map(|slot| slot.data.len() as u64).sum::<u64>();
        self.progress.storage_bytes +=
            response.proof.iter().map(|proof| proof.len() as u64).sum::<u64>();

        PartialStateSnapEvent::StorageRanges { request, peer_id, response, progress: self.progress }
    }

    /// Handles a bytecodes response and updates progress counters.
    fn on_bytecodes_response(
        &mut self,
        request: GetByteCodesMessage,
        peer_id: PeerId,
        response: ByteCodesMessage,
    ) -> PartialStateSnapEvent {
        self.progress.bytecode_responses += 1;
        self.progress.bytecodes += response.codes.len() as u64;
        self.progress.bytecode_bytes +=
            response.codes.iter().map(|code| code.len() as u64).sum::<u64>();

        PartialStateSnapEvent::ByteCodes { request, peer_id, response, progress: self.progress }
    }

    /// Queues storage and bytecode requests derived from the returned account leaves.
    fn queue_state_requests_from_accounts(
        &mut self,
        response: &AccountRangeMessage,
    ) -> Result<(), PartialStateSnapDownloaderError> {
        let mut storage_account_hashes = Vec::new();
        let mut bytecode_hashes = Vec::new();

        for account in &response.accounts {
            let trie_account =
                TrieAccount::decode(&mut account.body.as_ref()).map_err(|source| {
                    PartialStateSnapDownloaderError::AccountDecode {
                        account_hash: account.hash,
                        source,
                    }
                })?;

            if trie_account.storage_root != EMPTY_ROOT_HASH {
                if self.should_sync_storage(account.hash) {
                    storage_account_hashes.push(account.hash);
                } else {
                    self.progress.storage_skipped += 1;
                }
            }
            if trie_account.code_hash != KECCAK_EMPTY {
                if self.should_sync_code(account.hash) {
                    bytecode_hashes.push(trie_account.code_hash);
                } else {
                    self.progress.bytecodes_skipped += 1;
                }
            }
        }

        if !storage_account_hashes.is_empty() {
            let target = self.target.ok_or(PartialStateSnapDownloaderError::MissingTarget)?;
            self.pending_storage_ranges.push_back(GetStorageRangesMessage {
                request_id: 0,
                root_hash: target.root_hash,
                account_hashes: storage_account_hashes,
                starting_hash: B256::ZERO,
                limit_hash: B256::repeat_byte(0xff),
                response_bytes: self.config.response_bytes,
            });
        }
        if !bytecode_hashes.is_empty() {
            self.pending_bytecodes.push_back(GetByteCodesMessage {
                request_id: 0,
                hashes: bytecode_hashes,
                response_bytes: self.config.response_bytes,
            });
        }

        Ok(())
    }

    /// Returns true if any derived storage or bytecode work is queued or in flight.
    fn has_state_work(&self) -> bool {
        !self.pending_storage_ranges.is_empty() ||
            !self.pending_bytecodes.is_empty() ||
            self.in_flight_storage_ranges.is_some() ||
            self.in_flight_bytecodes.is_some()
    }

    /// Returns whether storage should be downloaded for the account hash.
    fn should_sync_storage(&self, account_hash: B256) -> bool {
        self.filter.as_ref().is_none_or(|filter| filter.should_sync_storage_by_hash(&account_hash))
    }

    /// Returns whether bytecode should be downloaded for the account hash.
    fn should_sync_code(&self, account_hash: B256) -> bool {
        self.filter.as_ref().is_none_or(|filter| filter.should_sync_code_by_hash(&account_hash))
    }

    /// Queues a storage request from the pending queue if none is currently in flight.
    fn queue_storage_ranges(&mut self) {
        if self.in_flight_storage_ranges.is_none() {
            if let Some(request) = self.pending_storage_ranges.pop_front() {
                let fut =
                    self.client.get_storage_ranges_with_priority(request.clone(), Priority::High);
                self.in_flight_storage_ranges = Some(StorageRangesRequestFuture { request, fut });
            }
        }
    }

    /// Queues a bytecodes request from the pending queue if none is currently in flight.
    fn queue_bytecodes(&mut self) {
        if self.in_flight_bytecodes.is_none() {
            if let Some(request) = self.pending_bytecodes.pop_front() {
                let fut = self.client.get_byte_codes_with_priority(request.clone(), Priority::High);
                self.in_flight_bytecodes = Some(ByteCodesRequestFuture { request, fut });
            }
        }
    }
}

impl<C, F> Stream for PartialStateSnapDownloader<C, F>
where
    C: SnapClient + Unpin,
    F: ContractFilter + Unpin,
{
    type Item = Result<PartialStateSnapEvent, PartialStateSnapDownloaderError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.finished && !this.has_state_work() {
            return Poll::Ready(None)
        }

        this.queue_storage_ranges();
        this.queue_bytecodes();

        if let Some(mut request) = this.in_flight_storage_ranges.take() {
            match request.poll_unpin(cx) {
                Poll::Ready(StorageRangesRequestOutcome { request, outcome }) => match outcome {
                    Ok(response) => {
                        let (peer_id, response) = response.split();
                        return match response {
                            SnapResponse::StorageRanges(response) => Poll::Ready(Some(Ok(
                                this.on_storage_ranges_response(request, peer_id, response)
                            ))),
                            SnapResponse::AccountRange(_) => Poll::Ready(Some(Err(
                                PartialStateSnapDownloaderError::UnexpectedResponse(
                                    "account range",
                                ),
                            ))),
                            SnapResponse::ByteCodes(_) => Poll::Ready(Some(Err(
                                PartialStateSnapDownloaderError::UnexpectedResponse("bytecodes"),
                            ))),
                            SnapResponse::TrieNodes(_) => Poll::Ready(Some(Err(
                                PartialStateSnapDownloaderError::UnexpectedResponse("trie nodes"),
                            ))),
                        }
                    }
                    Err(error) => return Poll::Ready(Some(Err(error.into()))),
                },
                Poll::Pending => {
                    this.in_flight_storage_ranges = Some(request);
                }
            }
        }

        if let Some(mut request) = this.in_flight_bytecodes.take() {
            match request.poll_unpin(cx) {
                Poll::Ready(ByteCodesRequestOutcome { request, outcome }) => match outcome {
                    Ok(response) => {
                        let (peer_id, response) = response.split();
                        return match response {
                            SnapResponse::ByteCodes(response) => Poll::Ready(Some(Ok(
                                this.on_bytecodes_response(request, peer_id, response)
                            ))),
                            SnapResponse::AccountRange(_) => Poll::Ready(Some(Err(
                                PartialStateSnapDownloaderError::UnexpectedResponse(
                                    "account range",
                                ),
                            ))),
                            SnapResponse::StorageRanges(_) => Poll::Ready(Some(Err(
                                PartialStateSnapDownloaderError::UnexpectedResponse(
                                    "storage ranges",
                                ),
                            ))),
                            SnapResponse::TrieNodes(_) => Poll::Ready(Some(Err(
                                PartialStateSnapDownloaderError::UnexpectedResponse("trie nodes"),
                            ))),
                        }
                    }
                    Err(error) => return Poll::Ready(Some(Err(error.into()))),
                },
                Poll::Pending => {
                    this.in_flight_bytecodes = Some(request);
                }
            }
        }

        if this.has_state_work() {
            return Poll::Pending
        }

        if let Err(error) = this.queue_next_account_range() {
            return Poll::Ready(Some(Err(error)))
        }

        if this.finished {
            return Poll::Ready(None)
        }

        let Some(mut request) = this.in_flight_account_range.take() else { return Poll::Pending };

        match request.poll_unpin(cx) {
            Poll::Ready(AccountRangeRequestOutcome { request, outcome }) => match outcome {
                Ok(response) => {
                    let (peer_id, response) = response.split();
                    match response {
                        SnapResponse::AccountRange(response) => Poll::Ready(Some(
                            this.on_account_range_response(request, peer_id, response),
                        )),
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

impl PartialStateSnapProgress {
    const fn new() -> Self {
        Self {
            accounts: 0,
            account_bytes: 0,
            account_proofs: 0,
            account_proof_bytes: 0,
            storage_range_responses: 0,
            storage_slots: 0,
            storage_bytes: 0,
            storage_skipped: 0,
            bytecode_responses: 0,
            bytecodes: 0,
            bytecode_bytes: 0,
            bytecodes_skipped: 0,
            account_range_responses: 0,
        }
    }
}

/// Wraps a partial-state snap downloader and persists successful responses before yielding them.
#[must_use = "Stream does nothing unless polled"]
#[derive(Debug)]
pub struct PersistedPartialStateSnapDownloader<D, W> {
    downloader: D,
    writer: W,
}

impl<D, W> PersistedPartialStateSnapDownloader<D, W> {
    /// Creates a new persisted downloader wrapper.
    pub const fn new(downloader: D, writer: W) -> Self {
        Self { downloader, writer }
    }

    /// Returns the wrapped downloader and writer.
    pub fn into_parts(self) -> (D, W) {
        (self.downloader, self.writer)
    }
}

impl<C, F> PartialStateSnapDownloader<C, F>
where
    C: SnapClient,
    F: ContractFilter,
{
    /// Persists successful downloader events with the provided writer.
    pub const fn persist_with<W>(self, writer: W) -> PersistedPartialStateSnapDownloader<Self, W> {
        PersistedPartialStateSnapDownloader::new(self, writer)
    }
}

/// Error returned while persisting partial snap state responses.
#[derive(Debug, Error)]
pub enum PartialStateSnapPersistenceError<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    /// Downloader error.
    #[error(transparent)]
    Downloader(#[from] PartialStateSnapDownloaderError),
    /// Writer error.
    #[error("partial-state snap writer failed: {0}")]
    Writer(E),
    /// Account leaf body failed to decode while persisting.
    #[error("failed to decode snap account {account_hash} while persisting")]
    AccountDecode {
        /// Account hash whose body failed to decode.
        account_hash: B256,
        /// RLP decoding error.
        source: alloy_rlp::Error,
    },
    /// Storage slot body failed to decode while persisting.
    #[error("failed to decode snap storage slot {slot_hash} for account {account_hash}")]
    StorageDecode {
        /// Account hash whose storage slot failed to decode.
        account_hash: B256,
        /// Storage slot hash whose body failed to decode.
        slot_hash: B256,
        /// RLP decoding error.
        source: alloy_rlp::Error,
    },
    /// A snap response contained more records than its original request can identify.
    #[error("{kind} response contains {got} records, but request identifies {expected}")]
    ResponseLengthMismatch {
        /// Response kind.
        kind: &'static str,
        /// Number of identifiers available in the request.
        expected: usize,
        /// Number of records returned by the response.
        got: usize,
    },
}

impl<D, W> Stream for PersistedPartialStateSnapDownloader<D, W>
where
    D: Stream<Item = Result<PartialStateSnapEvent, PartialStateSnapDownloaderError>> + Unpin,
    W: PartialStateSnapWriter + Unpin,
    W::Error: std::error::Error + Send + Sync + 'static,
{
    type Item = Result<PartialStateSnapEvent, PartialStateSnapPersistenceError<W::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match ready!(Pin::new(&mut this.downloader).poll_next(cx)) {
            Some(Ok(event)) => {
                if let Err(error) = persist_snap_event(&mut this.writer, &event) {
                    return Poll::Ready(Some(Err(error)))
                }
                Poll::Ready(Some(Ok(event)))
            }
            Some(Err(error)) => Poll::Ready(Some(Err(error.into()))),
            None => Poll::Ready(None),
        }
    }
}

/// Persists a single partial snap event.
pub fn persist_snap_event<W>(
    writer: &mut W,
    event: &PartialStateSnapEvent,
) -> Result<(), PartialStateSnapPersistenceError<W::Error>>
where
    W: PartialStateSnapWriter,
    W::Error: std::error::Error + Send + Sync + 'static,
{
    match event {
        PartialStateSnapEvent::AccountRange { response, .. } => {
            for account in &response.accounts {
                let decoded =
                    TrieAccount::decode(&mut account.body.as_ref()).map_err(|source| {
                        PartialStateSnapPersistenceError::AccountDecode {
                            account_hash: account.hash,
                            source,
                        }
                    })?;
                writer
                    .write_account(account.hash, decoded)
                    .map_err(PartialStateSnapPersistenceError::Writer)?;
            }
        }
        PartialStateSnapEvent::StorageRanges { request, response, .. } => {
            if response.slots.len() > request.account_hashes.len() {
                return Err(PartialStateSnapPersistenceError::ResponseLengthMismatch {
                    kind: "storage ranges",
                    expected: request.account_hashes.len(),
                    got: response.slots.len(),
                })
            }
            for (account_hash, slots) in request.account_hashes.iter().copied().zip(&response.slots)
            {
                for slot in slots {
                    let value = U256::decode(&mut slot.data.as_ref()).map_err(|source| {
                        PartialStateSnapPersistenceError::StorageDecode {
                            account_hash,
                            slot_hash: slot.hash,
                            source,
                        }
                    })?;
                    writer
                        .write_storage(account_hash, slot.hash, value)
                        .map_err(PartialStateSnapPersistenceError::Writer)?;
                }
            }
        }
        PartialStateSnapEvent::ByteCodes { request, response, .. } => {
            if response.codes.len() > request.hashes.len() {
                return Err(PartialStateSnapPersistenceError::ResponseLengthMismatch {
                    kind: "bytecodes",
                    expected: request.hashes.len(),
                    got: response.codes.len(),
                })
            }
            for (code_hash, bytecode) in request.hashes.iter().copied().zip(&response.codes) {
                writer
                    .write_bytecode(code_hash, bytecode.as_ref())
                    .map_err(PartialStateSnapPersistenceError::Writer)?;
            }
        }
    }
    Ok(())
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

/// Future returned for a storage-ranges request.
#[derive(Debug)]
struct StorageRangesRequestFuture<F> {
    request: GetStorageRangesMessage,
    fut: F,
}

impl<F> Future for StorageRangesRequestFuture<F>
where
    F: Future<Output = PeerRequestResult<SnapResponse>> + Send + Sync + Unpin,
{
    type Output = StorageRangesRequestOutcome;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let outcome = ready!(this.fut.poll_unpin(cx));

        Poll::Ready(StorageRangesRequestOutcome { request: this.request.clone(), outcome })
    }
}

/// Storage-ranges request outcome.
#[derive(Debug)]
struct StorageRangesRequestOutcome {
    #[allow(dead_code)]
    request: GetStorageRangesMessage,
    outcome: PeerRequestResult<SnapResponse>,
}

/// Future returned for a bytecodes request.
#[derive(Debug)]
struct ByteCodesRequestFuture<F> {
    request: GetByteCodesMessage,
    fut: F,
}

impl<F> Future for ByteCodesRequestFuture<F>
where
    F: Future<Output = PeerRequestResult<SnapResponse>> + Send + Sync + Unpin,
{
    type Output = ByteCodesRequestOutcome;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let outcome = ready!(this.fut.poll_unpin(cx));

        Poll::Ready(ByteCodesRequestOutcome { request: this.request.clone(), outcome })
    }
}

/// Bytecodes request outcome.
#[derive(Debug)]
struct ByteCodesRequestOutcome {
    #[allow(dead_code)]
    request: GetByteCodesMessage,
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
    use alloy_primitives::{Address, Bytes, U256};
    use futures_util::StreamExt;
    use reth_eth_wire_types::snap::{
        AccountData, ByteCodesMessage, GetByteCodesMessage, GetStorageRangesMessage,
        GetTrieNodesMessage, StorageData, StorageRangesMessage, TrieNodesMessage,
    };
    use reth_network_p2p::download::DownloadClient;
    use reth_network_peers::WithPeerId;
    use reth_storage_api::ContractFilter;
    use std::{
        collections::{BTreeSet, VecDeque},
        convert::Infallible,
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
        let request = GetAccountRangeMessage {
            request_id: 0,
            root_hash: root,
            starting_hash: B256::ZERO,
            limit_hash: B256::repeat_byte(0xff),
            response_bytes: DEFAULT_PARTIAL_STATE_SNAP_RESPONSE_BYTES,
        };
        assert_eq!(client.account_range_requests(), vec![request.clone()]);
        assert_eq!(
            event,
            PartialStateSnapEvent::AccountRange {
                request,
                peer_id: peer,
                response,
                progress: PartialStateSnapProgress {
                    account_proofs: 1,
                    account_proof_bytes: 1,
                    account_range_responses: 1,
                    ..Default::default()
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
        let account_body = encoded_account(EMPTY_ROOT_HASH, KECCAK_EMPTY);
        let client = MockSnapClient::new([
            Ok(WithPeerId::new(
                peer,
                SnapResponse::AccountRange(AccountRangeMessage {
                    request_id: 0,
                    accounts: vec![AccountData { hash: first_hash, body: account_body.clone() }],
                    proof: vec![],
                }),
            )),
            Ok(WithPeerId::new(
                peer,
                SnapResponse::AccountRange(AccountRangeMessage {
                    request_id: 0,
                    accounts: vec![AccountData { hash: second_hash, body: account_body.clone() }],
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
            PartialStateSnapProgress {
                accounts: 2,
                account_bytes: account_body.len() as u64 * 2,
                account_range_responses: 2,
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn downloads_tracked_state_and_counts_skips() {
        let root = B256::repeat_byte(0x11);
        let peer = PeerId::repeat_byte(0x22);
        let tracked_hash = B256::with_last_byte(1);
        let untracked_hash = B256::with_last_byte(2);
        let tracked_storage_root = B256::repeat_byte(0x33);
        let tracked_code_hash = B256::repeat_byte(0x44);
        let untracked_storage_root = B256::repeat_byte(0x55);
        let untracked_code_hash = B256::repeat_byte(0x66);
        let storage_response = StorageRangesMessage {
            request_id: 0,
            slots: vec![vec![StorageData {
                hash: B256::repeat_byte(0x77),
                data: Bytes::from_static(&[0xaa, 0xbb]),
            }]],
            proof: vec![Bytes::from_static(&[0xcc])],
        };
        let bytecodes_response =
            ByteCodesMessage { request_id: 0, codes: vec![Bytes::from_static(&[0x60, 0x00])] };
        let client = MockSnapClient::new([
            Ok(WithPeerId::new(
                peer,
                SnapResponse::AccountRange(AccountRangeMessage {
                    request_id: 0,
                    accounts: vec![
                        AccountData {
                            hash: tracked_hash,
                            body: encoded_account(tracked_storage_root, tracked_code_hash),
                        },
                        AccountData {
                            hash: untracked_hash,
                            body: encoded_account(untracked_storage_root, untracked_code_hash),
                        },
                    ],
                    proof: vec![],
                }),
            )),
            Ok(WithPeerId::new(peer, SnapResponse::StorageRanges(storage_response.clone()))),
            Ok(WithPeerId::new(peer, SnapResponse::ByteCodes(bytecodes_response.clone()))),
        ]);
        let mut downloader = PartialStateSnapDownloader::with_filter(
            client.clone(),
            PartialStateSnapDownloaderConfig { response_bytes: 1024 },
            TestHashFilter::new([tracked_hash]),
        );

        downloader.start(PartialStateSnapTarget {
            root_hash: root,
            starting_hash: tracked_hash,
            limit_hash: untracked_hash,
        });

        assert!(matches!(
            downloader.next().await.unwrap().unwrap(),
            PartialStateSnapEvent::AccountRange { .. }
        ));
        assert_eq!(downloader.progress().storage_skipped, 1);
        assert_eq!(downloader.progress().bytecodes_skipped, 1);

        let storage_event = downloader.next().await.unwrap().unwrap();
        let storage_request = GetStorageRangesMessage {
            request_id: 0,
            root_hash: root,
            account_hashes: vec![tracked_hash],
            starting_hash: B256::ZERO,
            limit_hash: B256::repeat_byte(0xff),
            response_bytes: 1024,
        };
        let bytecode_request = GetByteCodesMessage {
            request_id: 0,
            hashes: vec![tracked_code_hash],
            response_bytes: 1024,
        };
        assert_eq!(client.storage_range_requests(), vec![storage_request.clone()]);
        assert_eq!(client.bytecode_requests(), vec![bytecode_request.clone()]);
        assert_eq!(
            storage_event,
            PartialStateSnapEvent::StorageRanges {
                request: storage_request,
                peer_id: peer,
                response: storage_response,
                progress: PartialStateSnapProgress {
                    accounts: 2,
                    account_bytes: encoded_account(tracked_storage_root, tracked_code_hash).len()
                        as u64 +
                        encoded_account(untracked_storage_root, untracked_code_hash).len() as u64,
                    storage_range_responses: 1,
                    storage_slots: 1,
                    storage_bytes: 3,
                    storage_skipped: 1,
                    bytecodes_skipped: 1,
                    account_range_responses: 1,
                    ..Default::default()
                },
            }
        );

        let bytecodes_event = downloader.next().await.unwrap().unwrap();
        assert_eq!(
            bytecodes_event,
            PartialStateSnapEvent::ByteCodes {
                request: bytecode_request,
                peer_id: peer,
                response: bytecodes_response,
                progress: PartialStateSnapProgress {
                    accounts: 2,
                    account_bytes: encoded_account(tracked_storage_root, tracked_code_hash).len()
                        as u64 +
                        encoded_account(untracked_storage_root, untracked_code_hash).len() as u64,
                    storage_range_responses: 1,
                    storage_slots: 1,
                    storage_bytes: 3,
                    storage_skipped: 1,
                    bytecode_responses: 1,
                    bytecodes: 1,
                    bytecode_bytes: 2,
                    bytecodes_skipped: 1,
                    account_range_responses: 1,
                    ..Default::default()
                },
            }
        );
        assert!(downloader.next().await.is_none());
    }

    #[tokio::test]
    async fn persisted_downloader_writes_successful_events() {
        let root = B256::repeat_byte(0x11);
        let peer = PeerId::repeat_byte(0x22);
        let account_hash = B256::with_last_byte(1);
        let storage_root = B256::repeat_byte(0x33);
        let code_hash = B256::repeat_byte(0x44);
        let slot_hash = B256::repeat_byte(0x55);
        let account_body = encoded_account(storage_root, code_hash);
        let storage_value = U256::from(0xaabbu64);
        let encoded_storage_value = Bytes::from(alloy_rlp::encode(storage_value));
        let bytecode = Bytes::from_static(&[0x60, 0x00]);
        let client = MockSnapClient::new([
            Ok(WithPeerId::new(
                peer,
                SnapResponse::AccountRange(AccountRangeMessage {
                    request_id: 0,
                    accounts: vec![AccountData { hash: account_hash, body: account_body.clone() }],
                    proof: vec![],
                }),
            )),
            Ok(WithPeerId::new(
                peer,
                SnapResponse::StorageRanges(StorageRangesMessage {
                    request_id: 0,
                    slots: vec![vec![StorageData { hash: slot_hash, data: encoded_storage_value }]],
                    proof: vec![],
                }),
            )),
            Ok(WithPeerId::new(
                peer,
                SnapResponse::ByteCodes(ByteCodesMessage {
                    request_id: 0,
                    codes: vec![bytecode.clone()],
                }),
            )),
        ]);
        let mut downloader = PartialStateSnapDownloader::with_filter(
            client,
            PartialStateSnapDownloaderConfig { response_bytes: 1024 },
            TestHashFilter::new([account_hash]),
        );
        downloader.start(PartialStateSnapTarget {
            root_hash: root,
            starting_hash: account_hash,
            limit_hash: account_hash,
        });
        let writer = RecordingSnapWriter::default();
        let records = writer.records.clone();
        let mut persisted = downloader.persist_with(writer);

        while let Some(event) = persisted.next().await {
            event.unwrap();
        }

        let records = records.lock().unwrap();
        assert_eq!(
            records.accounts,
            vec![RecordedAccount {
                account_hash,
                account: TrieAccount { nonce: 0, balance: U256::ZERO, storage_root, code_hash },
            }]
        );
        assert_eq!(
            records.storage,
            vec![RecordedStorage { account_hash, slot_hash, value: storage_value }]
        );
        assert_eq!(
            records.bytecodes,
            vec![RecordedBytecode { code_hash, bytecode: bytecode.to_vec() }]
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

    #[derive(Debug, Clone, Default)]
    struct RecordingSnapWriter {
        records: Arc<Mutex<RecordedSnapState>>,
    }

    impl PartialStateSnapWriter for RecordingSnapWriter {
        type Error = Infallible;

        fn write_account(
            &mut self,
            account_hash: B256,
            account: TrieAccount,
        ) -> Result<(), Self::Error> {
            self.records.lock().unwrap().accounts.push(RecordedAccount { account_hash, account });
            Ok(())
        }

        fn write_storage(
            &mut self,
            account_hash: B256,
            slot_hash: B256,
            value: U256,
        ) -> Result<(), Self::Error> {
            self.records.lock().unwrap().storage.push(RecordedStorage {
                account_hash,
                slot_hash,
                value,
            });
            Ok(())
        }

        fn write_bytecode(&mut self, code_hash: B256, bytecode: &[u8]) -> Result<(), Self::Error> {
            self.records
                .lock()
                .unwrap()
                .bytecodes
                .push(RecordedBytecode { code_hash, bytecode: bytecode.to_vec() });
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct RecordedSnapState {
        accounts: Vec<RecordedAccount>,
        storage: Vec<RecordedStorage>,
        bytecodes: Vec<RecordedBytecode>,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct RecordedAccount {
        account_hash: B256,
        account: TrieAccount,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct RecordedStorage {
        account_hash: B256,
        slot_hash: B256,
        value: U256,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct RecordedBytecode {
        code_hash: B256,
        bytecode: Vec<u8>,
    }

    #[derive(Clone, Default)]
    struct MockSnapClient {
        responses: Arc<Mutex<VecDeque<PeerRequestResult<SnapResponse>>>>,
        account_range_requests: Arc<Mutex<Vec<GetAccountRangeMessage>>>,
        storage_range_requests: Arc<Mutex<Vec<GetStorageRangesMessage>>>,
        bytecode_requests: Arc<Mutex<Vec<GetByteCodesMessage>>>,
    }

    impl MockSnapClient {
        fn new(responses: impl IntoIterator<Item = PeerRequestResult<SnapResponse>>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into_iter().collect())),
                account_range_requests: Default::default(),
                storage_range_requests: Default::default(),
                bytecode_requests: Default::default(),
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

        fn storage_range_requests(&self) -> Vec<GetStorageRangesMessage> {
            self.storage_range_requests.lock().unwrap().clone()
        }

        fn bytecode_requests(&self) -> Vec<GetByteCodesMessage> {
            self.bytecode_requests.lock().unwrap().clone()
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
            request: GetStorageRangesMessage,
            _priority: Priority,
        ) -> Self::Output {
            self.storage_range_requests.lock().unwrap().push(request);
            self.next_response()
        }

        fn get_byte_codes(&self, request: GetByteCodesMessage) -> Self::Output {
            self.get_byte_codes_with_priority(request, Priority::Normal)
        }

        fn get_byte_codes_with_priority(
            &self,
            request: GetByteCodesMessage,
            _priority: Priority,
        ) -> Self::Output {
            self.bytecode_requests.lock().unwrap().push(request);
            self.next_response()
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

    #[derive(Debug, Clone, Default)]
    struct TestHashFilter {
        tracked: BTreeSet<B256>,
    }

    impl TestHashFilter {
        fn new(hashes: impl IntoIterator<Item = B256>) -> Self {
            Self { tracked: hashes.into_iter().collect() }
        }
    }

    impl ContractFilter for TestHashFilter {
        fn should_sync_storage(&self, _address: &Address) -> bool {
            false
        }

        fn should_sync_code(&self, _address: &Address) -> bool {
            false
        }

        fn should_sync_storage_by_hash(&self, account_hash: &B256) -> bool {
            self.tracked.contains(account_hash)
        }

        fn should_sync_code_by_hash(&self, account_hash: &B256) -> bool {
            self.tracked.contains(account_hash)
        }

        fn is_tracked(&self, _address: &Address) -> bool {
            false
        }
    }

    fn encoded_account(storage_root: B256, code_hash: B256) -> Bytes {
        Bytes::from(alloy_rlp::encode(TrieAccount {
            nonce: 0,
            balance: U256::ZERO,
            storage_root,
            code_hash,
        }))
    }
}
