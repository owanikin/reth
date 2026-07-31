//! Blocks/Headers management for the p2p network.

use crate::{
    budget::DEFAULT_BUDGET_TRY_DRAIN_DOWNLOADERS, metered_poll_nested_stream_with_budget,
    metrics::EthRequestHandlerMetrics,
};
use alloy_consensus::{BlockHeader, ReceiptWithBloom};
use alloy_eips::BlockHashOrNumber;
use alloy_rlp::Encodable;
use futures::StreamExt;
use reth_eth_wire::{
    BlockAccessLists, BlockBodies, BlockHeaders, Cells, EthNetworkPrimitives, GetBlockAccessLists,
    GetBlockBodies, GetBlockHeaders, GetCells, GetNodeData, GetReceipts, GetReceipts70,
    HeadersDirection, NetworkPrimitives, NodeData, Receipts, Receipts69, Receipts70,
};
use reth_eth_wire_types::snap::{
    AccountData, AccountRangeMessage, ByteCodesMessage, GetAccountRangeMessage,
    GetByteCodesMessage, GetStorageRangesMessage, GetTrieNodesMessage, StorageData,
    StorageRangesMessage, TrieNodesMessage,
};
use reth_network_api::test_utils::PeersHandle;
use reth_network_p2p::error::RequestResult;
use reth_network_peers::PeerId;
use reth_primitives_traits::Block;
use reth_storage_api::{
    BalProvider, BlockReader, GetBlockAccessListLimit, HeaderProvider, PartialStateSnapProvider,
    PartialStateSnapTriePath,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{mpsc::Receiver, oneshot};
use tokio_stream::wrappers::ReceiverStream;

// Limits: <https://github.com/ethereum/go-ethereum/blob/b0d44338bbcefee044f1f635a84487cbbd8f0538/eth/protocols/eth/handler.go#L34-L56>

/// Maximum number of receipts to serve.
///
/// Used to limit lookups.
pub const MAX_RECEIPTS_SERVE: usize = 1024;

/// Maximum number of block headers to serve.
///
/// Used to limit lookups.
pub const MAX_HEADERS_SERVE: usize = 1024;

/// Maximum number of block headers to serve.
///
/// Used to limit lookups. With 24KB block sizes nowadays, the practical limit will always be
/// `SOFT_RESPONSE_LIMIT`.
pub const MAX_BODIES_SERVE: usize = 1024;

/// Maximum number of block access lists to serve.
///
/// Used to limit lookups.
pub const MAX_BLOCK_ACCESS_LISTS_SERVE: usize = 1024;

/// Maximum size of replies to data retrievals: 2MB
pub const SOFT_RESPONSE_LIMIT: usize = 2 * 1024 * 1024;

/// Manages eth related requests on top of the p2p network.
///
/// This can be spawned to another task and is supposed to be run as background service.
#[derive(Debug)]
#[must_use = "Manager does nothing unless polled."]
pub struct EthRequestHandler<C, N: NetworkPrimitives = EthNetworkPrimitives> {
    /// The client type that can interact with the chain.
    client: C,
    /// Used for reporting peers.
    // TODO use to report spammers
    #[expect(dead_code)]
    peers: PeersHandle,
    /// Incoming request from the [`NetworkManager`](crate::NetworkManager).
    incoming_requests: ReceiverStream<IncomingEthRequest<N>>,
    /// Metrics for the eth request handler.
    metrics: EthRequestHandlerMetrics,
}

// === impl EthRequestHandler ===
impl<C, N: NetworkPrimitives> EthRequestHandler<C, N> {
    /// Create a new instance
    pub fn new(client: C, peers: PeersHandle, incoming: Receiver<IncomingEthRequest<N>>) -> Self {
        Self {
            client,
            peers,
            incoming_requests: ReceiverStream::new(incoming),
            metrics: Default::default(),
        }
    }
}

impl<C, N> EthRequestHandler<C, N>
where
    N: NetworkPrimitives,
    C: BlockReader,
{
    /// Returns the list of requested headers
    fn get_headers_response(&self, request: GetBlockHeaders) -> Vec<C::Header> {
        let GetBlockHeaders { start_block, limit, skip, direction } = request;

        let mut headers = Vec::new();

        let mut block: BlockHashOrNumber = match start_block {
            BlockHashOrNumber::Hash(start) => start.into(),
            BlockHashOrNumber::Number(num) => {
                let Some(hash) = self.client.block_hash(num).unwrap_or_default() else {
                    return headers
                };
                hash.into()
            }
        };

        let skip = skip as u64;
        let mut total_bytes = 0;

        for _ in 0..limit {
            if let Some(header) = self.client.header_by_hash_or_number(block).unwrap_or_default() {
                let number = header.number();
                let parent_hash = header.parent_hash();

                total_bytes += header.length();
                headers.push(header);

                if headers.len() >= MAX_HEADERS_SERVE || total_bytes > SOFT_RESPONSE_LIMIT {
                    break
                }

                match direction {
                    HeadersDirection::Rising => {
                        if let Some(next) = number.checked_add(1).and_then(|n| n.checked_add(skip))
                        {
                            block = next.into()
                        } else {
                            break
                        }
                    }
                    HeadersDirection::Falling => {
                        if skip > 0 {
                            // prevent under flows for block.number == 0 and `block.number - skip <
                            // 0`
                            if let Some(next) =
                                number.checked_sub(1).and_then(|num| num.checked_sub(skip))
                            {
                                block = next.into()
                            } else {
                                break
                            }
                        } else {
                            block = parent_hash.into()
                        }
                    }
                }
            } else {
                break
            }
        }

        headers
    }

    fn on_headers_request(
        &self,
        _peer_id: PeerId,
        request: GetBlockHeaders,
        response: oneshot::Sender<RequestResult<BlockHeaders<C::Header>>>,
    ) {
        self.metrics.eth_headers_requests_received_total.increment(1);
        let headers = self.get_headers_response(request);
        let _ = response.send(Ok(BlockHeaders(headers)));
    }

    fn on_bodies_request(
        &self,
        _peer_id: PeerId,
        request: GetBlockBodies,
        response: oneshot::Sender<RequestResult<BlockBodies<<C::Block as Block>::Body>>>,
    ) {
        self.metrics.eth_bodies_requests_received_total.increment(1);
        let mut bodies = Vec::new();

        let mut total_bytes = 0;

        for hash in request {
            if let Some(block) = self.client.block_by_hash(hash).unwrap_or_default() {
                let body = block.into_body();
                total_bytes += body.length();
                bodies.push(body);

                if bodies.len() >= MAX_BODIES_SERVE || total_bytes > SOFT_RESPONSE_LIMIT {
                    break
                }
            } else {
                break
            }
        }

        let _ = response.send(Ok(BlockBodies(bodies)));
    }

    fn on_receipts_request(
        &self,
        _peer_id: PeerId,
        request: GetReceipts,
        response: oneshot::Sender<RequestResult<Receipts<C::Receipt>>>,
    ) {
        self.metrics.eth_receipts_requests_received_total.increment(1);

        let receipts = self.get_receipts_response(request, |receipts_by_block| {
            receipts_by_block.into_iter().map(ReceiptWithBloom::from).collect::<Vec<_>>()
        });

        let _ = response.send(Ok(Receipts(receipts)));
    }

    fn on_receipts69_request(
        &self,
        _peer_id: PeerId,
        request: GetReceipts,
        response: oneshot::Sender<RequestResult<Receipts69<C::Receipt>>>,
    ) {
        self.metrics.eth_receipts_requests_received_total.increment(1);

        let receipts = self.get_receipts_response(request, |receipts_by_block| {
            // skip bloom filter for eth69
            receipts_by_block
        });

        let _ = response.send(Ok(Receipts69(receipts)));
    }

    /// Handles partial responses for [`GetReceipts70`] queries.
    ///
    /// This will adhere to the soft limit but allow filling the last vec partially.
    fn on_receipts70_request(
        &self,
        _peer_id: PeerId,
        request: GetReceipts70,
        response: oneshot::Sender<RequestResult<Receipts70<C::Receipt>>>,
    ) {
        self.metrics.eth_receipts_requests_received_total.increment(1);

        let GetReceipts70 { first_block_receipt_index, block_hashes } = request;

        let mut receipts = Vec::new();
        let mut total_bytes = 0usize;
        let mut last_block_incomplete = false;

        for (idx, hash) in block_hashes.into_iter().enumerate() {
            if idx >= MAX_RECEIPTS_SERVE {
                break
            }

            let Some(mut block_receipts) =
                self.client.receipts_by_block(BlockHashOrNumber::Hash(hash)).unwrap_or_default()
            else {
                break
            };

            if idx == 0 && first_block_receipt_index > 0 {
                let skip = first_block_receipt_index as usize;
                if skip >= block_receipts.len() {
                    block_receipts.clear();
                } else {
                    block_receipts.drain(0..skip);
                }
            }

            let block_size = block_receipts.length();

            if total_bytes + block_size <= SOFT_RESPONSE_LIMIT {
                total_bytes += block_size;
                receipts.push(block_receipts);
                continue;
            }

            let mut partial_block = Vec::new();
            for receipt in block_receipts {
                let receipt_size = receipt.length();
                if total_bytes + receipt_size > SOFT_RESPONSE_LIMIT {
                    break;
                }
                total_bytes += receipt_size;
                partial_block.push(receipt);
            }

            receipts.push(partial_block);
            last_block_incomplete = true;
            break;
        }

        let _ = response.send(Ok(Receipts70 { last_block_incomplete, receipts }));
    }

    #[inline]
    fn get_receipts_response<T, F>(&self, request: GetReceipts, transform_fn: F) -> Vec<Vec<T>>
    where
        F: Fn(Vec<C::Receipt>) -> Vec<T>,
        T: Encodable,
    {
        let mut receipts = Vec::new();
        let mut total_bytes = 0;

        for hash in request {
            if let Some(receipts_by_block) =
                self.client.receipts_by_block(BlockHashOrNumber::Hash(hash)).unwrap_or_default()
            {
                let transformed_receipts = transform_fn(receipts_by_block);
                total_bytes += transformed_receipts.length();
                receipts.push(transformed_receipts);

                if receipts.len() >= MAX_RECEIPTS_SERVE || total_bytes > SOFT_RESPONSE_LIMIT {
                    break
                }
            } else {
                break
            }
        }

        receipts
    }

    fn on_cells_request(
        &self,
        _peer_id: PeerId,
        _request: GetCells,
        response: oneshot::Sender<RequestResult<Cells>>,
    ) {
        let _ = response.send(Ok(Cells::default()));
    }
}

impl<C, N> EthRequestHandler<C, N>
where
    N: NetworkPrimitives,
    C: BalProvider,
{
    /// Handles [`GetBlockAccessLists`] queries.
    ///
    /// EIP-8159 defines the final `BlockAccessLists` response semantics:
    /// <https://eips.ethereum.org/EIPS/eip-8159>
    fn on_block_access_lists_request(
        &self,
        _peer_id: PeerId,
        mut request: GetBlockAccessLists,
        response: oneshot::Sender<RequestResult<BlockAccessLists>>,
    ) {
        request.0.truncate(MAX_BLOCK_ACCESS_LISTS_SERVE);

        let limit = GetBlockAccessListLimit::ResponseSizeSoftLimit(SOFT_RESPONSE_LIMIT);
        let access_lists =
            self.client.bal_store().get_by_hashes_with_limit(&request.0, limit).unwrap_or_default();
        let _ = response.send(Ok(BlockAccessLists(access_lists)));
    }
}

impl<C, N> EthRequestHandler<C, N>
where
    N: NetworkPrimitives,
    C: PartialStateSnapProvider,
{
    fn on_account_range_request(
        &self,
        peer_id: PeerId,
        request: GetAccountRangeMessage,
        response: oneshot::Sender<RequestResult<AccountRangeMessage>>,
    ) {
        let result = self.client.snap_account_range(
            request.root_hash,
            request.starting_hash,
            request.limit_hash,
            request.response_bytes,
        );

        let result = match result {
            Ok(range) => {
                tracing::debug!(
                    target: "net::eth",
                    ?peer_id,
                    root = ?request.root_hash,
                    start = ?request.starting_hash,
                    limit = ?request.limit_hash,
                    accounts = range.accounts.len(),
                    proof = range.proof.len(),
                    "Served snap account range"
                );
                AccountRangeMessage {
                    request_id: request.request_id,
                    accounts: range
                        .accounts
                        .into_iter()
                        .map(|account| AccountData {
                            hash: account.hash,
                            body: alloy_rlp::encode(account.account).into(),
                        })
                        .collect(),
                    proof: range.proof,
                }
            }
            Err(err) => {
                tracing::debug!(
                    target: "net::eth",
                    %err,
                    ?peer_id,
                    root = ?request.root_hash,
                    start = ?request.starting_hash,
                    limit = ?request.limit_hash,
                    "Failed to serve snap account range"
                );
                AccountRangeMessage {
                    request_id: request.request_id,
                    accounts: Vec::new(),
                    proof: Vec::new(),
                }
            }
        };

        let _ = response.send(Ok(result));
    }

    fn on_storage_ranges_request(
        &self,
        peer_id: PeerId,
        request: GetStorageRangesMessage,
        response: oneshot::Sender<RequestResult<StorageRangesMessage>>,
    ) {
        let result = self.client.snap_storage_ranges(
            request.root_hash,
            &request.account_hashes,
            request.starting_hash,
            request.limit_hash,
            request.response_bytes,
        );

        let result = match result {
            Ok(ranges) => {
                let slot_count = ranges.slots.iter().map(Vec::len).sum::<usize>();
                tracing::debug!(
                    target: "net::eth",
                    ?peer_id,
                    root = ?request.root_hash,
                    accounts = request.account_hashes.len(),
                    slots = slot_count,
                    proof = ranges.proof.len(),
                    "Served snap storage ranges"
                );
                StorageRangesMessage {
                    request_id: request.request_id,
                    slots: ranges
                        .slots
                        .into_iter()
                        .map(|slots| {
                            slots
                                .into_iter()
                                .map(|slot| StorageData {
                                    hash: slot.hash,
                                    data: alloy_rlp::encode(slot.value).into(),
                                })
                                .collect()
                        })
                        .collect(),
                    proof: ranges.proof,
                }
            }
            Err(err) => {
                tracing::debug!(
                    target: "net::eth",
                    %err,
                    ?peer_id,
                    root = ?request.root_hash,
                    accounts = request.account_hashes.len(),
                    start = ?request.starting_hash,
                    limit = ?request.limit_hash,
                    "Failed to serve snap storage ranges"
                );
                StorageRangesMessage {
                    request_id: request.request_id,
                    slots: Vec::new(),
                    proof: Vec::new(),
                }
            }
        };

        let _ = response.send(Ok(result));
    }

    fn on_bytecodes_request(
        &self,
        peer_id: PeerId,
        request: GetByteCodesMessage,
        response: oneshot::Sender<RequestResult<ByteCodesMessage>>,
    ) {
        let result = match self.client.snap_bytecodes(&request.hashes, request.response_bytes) {
            Ok(bytecodes) => {
                tracing::debug!(
                    target: "net::eth",
                    ?peer_id,
                    requested = request.hashes.len(),
                    codes = bytecodes.codes.len(),
                    "Served snap bytecodes"
                );
                ByteCodesMessage { request_id: request.request_id, codes: bytecodes.codes }
            }
            Err(err) => {
                tracing::debug!(
                    target: "net::eth",
                    %err,
                    ?peer_id,
                    hashes = request.hashes.len(),
                    "Failed to serve snap bytecodes"
                );
                ByteCodesMessage { request_id: request.request_id, codes: Vec::new() }
            }
        };

        let _ = response.send(Ok(result));
    }

    fn on_trie_nodes_request(
        &self,
        peer_id: PeerId,
        request: GetTrieNodesMessage,
        response: oneshot::Sender<RequestResult<TrieNodesMessage>>,
    ) {
        let paths = request
            .paths
            .iter()
            .map(|path| PartialStateSnapTriePath {
                account_path: path.account_path.clone(),
                slot_paths: path.slot_paths.clone(),
            })
            .collect::<Vec<_>>();
        let result =
            match self.client.snap_trie_nodes(request.root_hash, &paths, request.response_bytes) {
                Ok(nodes) => {
                    tracing::debug!(
                        target: "net::eth",
                        ?peer_id,
                        root = ?request.root_hash,
                        paths = request.paths.len(),
                        nodes = nodes.nodes.len(),
                        "Served snap trie nodes"
                    );
                    TrieNodesMessage { request_id: request.request_id, nodes: nodes.nodes }
                }
                Err(err) => {
                    tracing::debug!(
                        target: "net::eth",
                        %err,
                        ?peer_id,
                        root = ?request.root_hash,
                        paths = request.paths.len(),
                        "Failed to serve snap trie nodes"
                    );
                    TrieNodesMessage { request_id: request.request_id, nodes: Vec::new() }
                }
            };

        let _ = response.send(Ok(result));
    }
}

/// An endless future.
///
/// This should be spawned or used as part of `tokio::select!`.
impl<C, N> Future for EthRequestHandler<C, N>
where
    N: NetworkPrimitives,
    C: BalProvider
        + BlockReader<Block = N::Block, Receipt = N::Receipt>
        + HeaderProvider<Header = N::BlockHeader>
        + PartialStateSnapProvider
        + Unpin,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let mut acc = Duration::ZERO;
        let maybe_more_incoming_requests = metered_poll_nested_stream_with_budget!(
            acc,
            "net::eth",
            "Incoming eth requests stream",
            DEFAULT_BUDGET_TRY_DRAIN_DOWNLOADERS,
            this.incoming_requests.poll_next_unpin(cx),
            |incoming| {
                match incoming {
                    IncomingEthRequest::GetBlockHeaders { peer_id, request, response } => {
                        this.on_headers_request(peer_id, request, response)
                    }
                    IncomingEthRequest::GetBlockBodies { peer_id, request, response } => {
                        this.on_bodies_request(peer_id, request, response)
                    }
                    IncomingEthRequest::GetNodeData { .. } => {
                        this.metrics.eth_node_data_requests_received_total.increment(1);
                    }
                    IncomingEthRequest::GetReceipts { peer_id, request, response } => {
                        this.on_receipts_request(peer_id, request, response)
                    }
                    IncomingEthRequest::GetReceipts69 { peer_id, request, response } => {
                        this.on_receipts69_request(peer_id, request, response)
                    }
                    IncomingEthRequest::GetReceipts70 { peer_id, request, response } => {
                        this.on_receipts70_request(peer_id, request, response)
                    }
                    IncomingEthRequest::GetBlockAccessLists { peer_id, request, response } => {
                        this.on_block_access_lists_request(peer_id, request, response)
                    }
                    IncomingEthRequest::GetCells { peer_id, request, response } => {
                        this.on_cells_request(peer_id, request, response)
                    }
                    IncomingEthRequest::GetAccountRange { peer_id, request, response } => {
                        this.on_account_range_request(peer_id, request, response)
                    }
                    IncomingEthRequest::GetStorageRanges { peer_id, request, response } => {
                        this.on_storage_ranges_request(peer_id, request, response)
                    }
                    IncomingEthRequest::GetByteCodes { peer_id, request, response } => {
                        this.on_bytecodes_request(peer_id, request, response)
                    }
                    IncomingEthRequest::GetTrieNodes { peer_id, request, response } => {
                        this.on_trie_nodes_request(peer_id, request, response)
                    }
                }
            },
        );

        this.metrics.acc_duration_poll_eth_req_handler.set(acc.as_secs_f64());

        // stream is fully drained and import futures pending
        if maybe_more_incoming_requests {
            // make sure we're woken up again
            cx.waker().wake_by_ref();
        }

        Poll::Pending
    }
}

/// All `eth` request related to blocks delegated by the network.
#[derive(Debug)]
pub enum IncomingEthRequest<N: NetworkPrimitives = EthNetworkPrimitives> {
    /// Request Block headers from the peer.
    ///
    /// The response should be sent through the channel.
    GetBlockHeaders {
        /// The ID of the peer to request block headers from.
        peer_id: PeerId,
        /// The specific block headers requested.
        request: GetBlockHeaders,
        /// The channel sender for the response containing block headers.
        response: oneshot::Sender<RequestResult<BlockHeaders<N::BlockHeader>>>,
    },
    /// Request Block bodies from the peer.
    ///
    /// The response should be sent through the channel.
    GetBlockBodies {
        /// The ID of the peer to request block bodies from.
        peer_id: PeerId,
        /// The specific block bodies requested.
        request: GetBlockBodies,
        /// The channel sender for the response containing block bodies.
        response: oneshot::Sender<RequestResult<BlockBodies<N::BlockBody>>>,
    },
    /// Request Node Data from the peer.
    ///
    /// The response should be sent through the channel.
    GetNodeData {
        /// The ID of the peer to request node data from.
        peer_id: PeerId,
        /// The specific node data requested.
        request: GetNodeData,
        /// The channel sender for the response containing node data.
        response: oneshot::Sender<RequestResult<NodeData>>,
    },
    /// Request Receipts from the peer.
    ///
    /// The response should be sent through the channel.
    GetReceipts {
        /// The ID of the peer to request receipts from.
        peer_id: PeerId,
        /// The specific receipts requested.
        request: GetReceipts,
        /// The channel sender for the response containing receipts.
        response: oneshot::Sender<RequestResult<Receipts<N::Receipt>>>,
    },
    /// Request Receipts from the peer without bloom filter.
    ///
    /// The response should be sent through the channel.
    GetReceipts69 {
        /// The ID of the peer to request receipts from.
        peer_id: PeerId,
        /// The specific receipts requested.
        request: GetReceipts,
        /// The channel sender for the response containing Receipts69.
        response: oneshot::Sender<RequestResult<Receipts69<N::Receipt>>>,
    },
    /// Request Receipts from the peer using eth/70.
    ///
    /// The response should be sent through the channel.
    GetReceipts70 {
        /// The ID of the peer to request receipts from.
        peer_id: PeerId,
        /// The specific receipts requested including the `firstBlockReceiptIndex`.
        request: GetReceipts70,
        /// The channel sender for the response containing Receipts70.
        response: oneshot::Sender<RequestResult<Receipts70<N::Receipt>>>,
    },
    /// Request Block Access Lists from the peer.
    ///
    /// The response should be sent through the channel.
    GetBlockAccessLists {
        /// The ID of the peer to request block access lists from.
        peer_id: PeerId,
        /// The requested block hashes.
        request: GetBlockAccessLists,
        /// The channel sender for the response containing block access lists.
        response: oneshot::Sender<RequestResult<BlockAccessLists>>,
    },
    /// Request Cells from the peer.
    ///
    /// The response should be sent through the channel.
    GetCells {
        /// The ID of the peer to request cells from.
        peer_id: PeerId,
        /// The requested block hashes.
        request: GetCells,
        /// The channel sender for the response containing cells.
        response: oneshot::Sender<RequestResult<Cells>>,
    },
    /// Request account range data through snap.
    GetAccountRange {
        /// The ID of the peer that requested account range data.
        peer_id: PeerId,
        /// The requested account range.
        request: GetAccountRangeMessage,
        /// The channel sender for the response containing account range data.
        response: oneshot::Sender<RequestResult<AccountRangeMessage>>,
    },
    /// Request storage range data through snap.
    GetStorageRanges {
        /// The ID of the peer that requested storage ranges.
        peer_id: PeerId,
        /// The requested storage ranges.
        request: GetStorageRangesMessage,
        /// The channel sender for the response containing storage range data.
        response: oneshot::Sender<RequestResult<StorageRangesMessage>>,
    },
    /// Request bytecodes through snap.
    GetByteCodes {
        /// The ID of the peer that requested bytecodes.
        peer_id: PeerId,
        /// The requested bytecodes.
        request: GetByteCodesMessage,
        /// The channel sender for the response containing bytecodes.
        response: oneshot::Sender<RequestResult<ByteCodesMessage>>,
    },
    /// Request trie nodes through snap.
    GetTrieNodes {
        /// The ID of the peer that requested trie nodes.
        peer_id: PeerId,
        /// The requested trie nodes.
        request: GetTrieNodesMessage,
        /// The channel sender for the response containing trie nodes.
        response: oneshot::Sender<RequestResult<TrieNodesMessage>>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
    use alloy_rlp::Decodable;
    use reth_eth_wire_types::snap::TriePath;
    use reth_storage_api::{
        errors::provider::ProviderResult, PartialStateSnapAccount, PartialStateSnapAccountRange,
        PartialStateSnapByteCodes, PartialStateSnapStorage, PartialStateSnapStorageRanges,
        PartialStateSnapTrieNodes,
    };
    use tokio::sync::mpsc;

    #[derive(Debug, Clone, Default)]
    struct TestSnapProvider {
        account_range: PartialStateSnapAccountRange,
        storage_ranges: PartialStateSnapStorageRanges,
        bytecodes: PartialStateSnapByteCodes,
        trie_nodes: PartialStateSnapTrieNodes,
    }

    impl PartialStateSnapProvider for TestSnapProvider {
        fn snap_account_range(
            &self,
            _root_hash: B256,
            _starting_hash: B256,
            _limit_hash: B256,
            _response_bytes: u64,
        ) -> ProviderResult<PartialStateSnapAccountRange> {
            Ok(self.account_range.clone())
        }

        fn snap_storage_ranges(
            &self,
            _root_hash: B256,
            _account_hashes: &[B256],
            _starting_hash: B256,
            _limit_hash: B256,
            _response_bytes: u64,
        ) -> ProviderResult<PartialStateSnapStorageRanges> {
            Ok(self.storage_ranges.clone())
        }

        fn snap_bytecodes(
            &self,
            _hashes: &[B256],
            _response_bytes: u64,
        ) -> ProviderResult<PartialStateSnapByteCodes> {
            Ok(self.bytecodes.clone())
        }

        fn snap_trie_nodes(
            &self,
            _root_hash: B256,
            _paths: &[reth_storage_api::PartialStateSnapTriePath],
            _response_bytes: u64,
        ) -> ProviderResult<PartialStateSnapTrieNodes> {
            Ok(self.trie_nodes.clone())
        }
    }

    fn test_handler(provider: TestSnapProvider) -> EthRequestHandler<TestSnapProvider> {
        let (peers_tx, _peers_rx) = mpsc::unbounded_channel();
        let (_incoming_tx, incoming_rx) = mpsc::channel(1);
        EthRequestHandler::new(provider, PeersHandle::new(peers_tx), incoming_rx)
    }

    fn decode_storage_value(data: &Bytes) -> U256 {
        let mut data = data.as_ref();
        U256::decode(&mut data).unwrap()
    }

    #[tokio::test]
    async fn serves_snap_account_range_from_provider() {
        let address = Address::repeat_byte(0x11);
        let provider = TestSnapProvider {
            account_range: PartialStateSnapAccountRange {
                accounts: vec![PartialStateSnapAccount {
                    hash: keccak256(address),
                    account: Default::default(),
                }],
                proof: Vec::new(),
            },
            ..Default::default()
        };

        let handler = test_handler(provider);
        let (tx, rx) = oneshot::channel();
        handler.on_account_range_request(
            PeerId::random(),
            GetAccountRangeMessage {
                request_id: 1,
                root_hash: B256::ZERO,
                starting_hash: B256::ZERO,
                limit_hash: B256::repeat_byte(0xff),
                response_bytes: SOFT_RESPONSE_LIMIT as u64,
            },
            tx,
        );

        let response = rx.await.unwrap().unwrap();
        assert_eq!(response.request_id, 1);
        assert_eq!(response.accounts.len(), 1);
        assert_eq!(response.accounts[0].hash, keccak256(address));
        assert!(!response.accounts[0].body.is_empty());
    }

    #[tokio::test]
    async fn serves_snap_storage_ranges_from_provider() {
        let address = Address::repeat_byte(0x22);
        let slot = B256::with_last_byte(1);
        let value = U256::from(42);
        let provider = TestSnapProvider {
            storage_ranges: PartialStateSnapStorageRanges {
                slots: vec![vec![PartialStateSnapStorage { hash: keccak256(slot), value }]],
                proof: Vec::new(),
            },
            ..Default::default()
        };

        let handler = test_handler(provider);
        let (tx, rx) = oneshot::channel();
        handler.on_storage_ranges_request(
            PeerId::random(),
            GetStorageRangesMessage {
                request_id: 2,
                root_hash: B256::ZERO,
                account_hashes: vec![keccak256(address)],
                starting_hash: B256::ZERO,
                limit_hash: B256::repeat_byte(0xff),
                response_bytes: SOFT_RESPONSE_LIMIT as u64,
            },
            tx,
        );

        let response = rx.await.unwrap().unwrap();
        assert_eq!(response.request_id, 2);
        assert_eq!(response.slots.len(), 1);
        assert_eq!(response.slots[0].len(), 1);
        assert_eq!(response.slots[0][0].hash, keccak256(slot));
        assert_eq!(decode_storage_value(&response.slots[0][0].data), value);
    }

    #[tokio::test]
    async fn serves_snap_bytecodes_from_provider() {
        let bytecode = Bytes::from(vec![0x60, 0x00, 0x60, 0x01]);
        let code_hash = keccak256(bytecode.as_ref());
        let provider = TestSnapProvider {
            bytecodes: PartialStateSnapByteCodes { codes: vec![bytecode.clone()] },
            ..Default::default()
        };

        let handler = test_handler(provider);
        let (tx, rx) = oneshot::channel();
        handler.on_bytecodes_request(
            PeerId::random(),
            GetByteCodesMessage {
                request_id: 3,
                hashes: vec![code_hash],
                response_bytes: SOFT_RESPONSE_LIMIT as u64,
            },
            tx,
        );

        let response = rx.await.unwrap().unwrap();
        assert_eq!(response.request_id, 3);
        assert_eq!(response.codes, vec![bytecode]);
    }

    #[tokio::test]
    async fn serves_empty_snap_trie_nodes_from_provider() {
        let handler = test_handler(TestSnapProvider::default());
        let (tx, rx) = oneshot::channel();
        handler.on_trie_nodes_request(
            PeerId::random(),
            GetTrieNodesMessage {
                request_id: 4,
                root_hash: B256::ZERO,
                paths: vec![TriePath {
                    account_path: Bytes::from(vec![0xab]),
                    slot_paths: vec![Bytes::from(vec![0xcd])],
                }],
                response_bytes: SOFT_RESPONSE_LIMIT as u64,
            },
            tx,
        );

        let response = rx.await.unwrap().unwrap();
        assert_eq!(response.request_id, 4);
        assert!(response.nodes.is_empty());
    }
}
