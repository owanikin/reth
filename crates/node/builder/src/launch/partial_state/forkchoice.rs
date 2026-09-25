use super::{
    advance_partial_state_to_target, commitment_is_unavailable, PartialStateAdvanceOutcome,
    PartialStateChain,
};
use alloy_consensus::BlockHeader;
use alloy_eips::{eip7928::bal::DecodedBal, BlockNumHash};
use alloy_primitives::{Sealed, B256};
use reth_network_p2p::{
    error::RequestError,
    headers::client::{HeadersClient, HeadersRequest},
    priority::Priority,
    snap::client::SnapClient,
    BlockAccessListsClient,
};
use reth_primitives_traits::SealedHeader;
use reth_provider::{providers::ProviderNodeTypes, HeaderProvider, ProviderFactory};
use reth_storage_api::{BalProvider, ConfiguredContractFilter, PartialStateSnapPivot};
use reth_tracing::tracing::warn;
use std::{
    collections::{BTreeMap, VecDeque},
    time::Duration,
};
use tokio::{
    sync::watch,
    time::{sleep_until, timeout, Instant},
};

/// Replays the CL-selected branch independently of execution progress. Root verification does
/// not validate execution or authorize an Engine API VALID response.
#[derive(Debug)]
pub(crate) struct PartialStateAdvancer<H> {
    targets: watch::Receiver<Option<B256>>,
    retry_at: Option<Instant>,
    headers: VecDeque<SealedHeader<H>>,
}

impl<H: reth_primitives_traits::BlockHeader> PartialStateAdvancer<H> {
    pub(crate) const fn new(targets: watch::Receiver<Option<B256>>) -> Self {
        Self { targets, retry_at: None, headers: VecDeque::new() }
    }

    pub(crate) async fn advance<N, Client>(
        &mut self,
        client: &Client,
        factory: &ProviderFactory<N>,
        headers: &impl HeaderProvider<Header = Client::Header>,
        filter: &ConfiguredContractFilter,
        head: &mut PartialStateSnapPivot,
        retention: u64,
    ) -> eyre::Result<PartialStateAdvanceOutcome>
    where
        N: ProviderNodeTypes + 'static,
        Client: SnapClient + HeadersClient<Header = H> + BlockAccessListsClient,
    {
        loop {
            if let Some(deadline) = self.retry_at.take() {
                tokio::select! {
                    result = self.targets.changed() => result?,
                    _ = sleep_until(deadline) => {},
                }
            }
            let target = *self.targets.borrow_and_update();
            let Some(target) = target.filter(|hash| *hash != head.block_hash) else {
                self.targets.changed().await?;
                continue
            };

            // Cancel pending peer requests when forkchoice changes. Replay commits a root and
            // its checkpoint together without yielding, so cancellation cannot split a commit.
            let targets = self.targets.clone();
            let result = tokio::select! {
                biased;
                changed = self.targets.changed() => {
                    changed?;
                    continue
                }
                result = reconcile(client, factory, headers, filter, head, target, retention, targets, &mut self.headers) => result,
            };
            match result {
                Err(err) if err.is::<TargetChanged>() => continue,
                Err(err) if unavailable(&err) => {
                    self.retry_at = Some(Instant::now() + Duration::from_secs(5));
                    warn!(target: "reth::cli", block_number = head.block_number,
                        block_hash = %head.block_hash, state_root = %head.state_root,
                        target_hash = %target, retry_seconds = 5, %err,
                        "Partial-state advancement deferred; waiting for peer data availability");
                    return Ok(PartialStateAdvanceOutcome::AwaitingCommitment)
                }
                Ok(
                    outcome @ (PartialStateAdvanceOutcome::TargetTooDistant |
                    PartialStateAdvanceOutcome::BootstrapRequired { .. } |
                    PartialStateAdvanceOutcome::ResyncRequired { .. }),
                ) => {
                    self.retry_at = Some(Instant::now() + Duration::from_secs(5));
                    return Ok(outcome)
                }
                result => return result,
            }
        }
    }
}

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
// Bound memory and work while resolving an unexecuted branch. Large gaps require a newer pivot.
const MAX_BRANCH_HEADERS: usize = 4096;

#[derive(Debug)]
struct DataUnavailable;

impl std::fmt::Display for DataUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("partial-state header or BAL is not yet available")
    }
}
impl std::error::Error for DataUnavailable {}

#[derive(Debug)]
struct TargetChanged;
impl std::fmt::Display for TargetChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("partial-state forkchoice target changed")
    }
}
impl std::error::Error for TargetChanged {}

fn unavailable(err: &eyre::Report) -> bool {
    err.is::<DataUnavailable>() ||
        commitment_is_unavailable(err) ||
        err.downcast_ref::<RequestError>().is_some_and(|err| {
            err.is_retryable() || matches!(err, RequestError::UnsupportedCapability)
        })
}

struct Branch<'a, N: ProviderNodeTypes, Client: HeadersClient> {
    tip: BlockNumHash,
    anchor: Option<PartialStateSnapPivot>,
    headers: BTreeMap<u64, SealedHeader<Client::Header>>,
    targets: watch::Receiver<Option<B256>>,
    factory: &'a ProviderFactory<N>,
    client: &'a Client,
}

impl<N: ProviderNodeTypes, Client: HeadersClient + BlockAccessListsClient> PartialStateChain
    for Branch<'_, N, Client>
{
    type Header = Client::Header;
    fn tip(&self) -> eyre::Result<BlockNumHash> {
        if *self.targets.borrow() != Some(self.tip.hash) {
            return Err(TargetChanged.into())
        }
        Ok(self.tip)
    }
    fn oldest_block(&self) -> u64 {
        self.anchor.map_or_else(
            || *self.headers.first_key_value().expect("nonempty branch").0,
            |anchor| anchor.block_number,
        )
    }
    fn hash(&self, number: u64) -> eyre::Result<Option<B256>> {
        Ok(self.headers.get(&number).map(|header| header.hash()).or_else(|| {
            self.anchor
                .filter(|anchor| anchor.block_number == number)
                .map(|anchor| anchor.block_hash)
        }))
    }
    fn replay_header(&self, number: u64) -> eyre::Result<Option<SealedHeader<Self::Header>>> {
        Ok(self.headers.get(&number).cloned())
    }
    async fn prepare_bal(&self, block: BlockNumHash, expected: B256) -> eyre::Result<()> {
        ensure_bal(self.client, self.factory, block, expected).await
    }
}

async fn reconcile<N, Client>(
    client: &Client,
    factory: &ProviderFactory<N>,
    local: &impl HeaderProvider<Header = Client::Header>,
    filter: &ConfiguredContractFilter,
    head: &mut PartialStateSnapPivot,
    target: B256,
    retention: u64,
    targets: watch::Receiver<Option<B256>>,
    cache: &mut VecDeque<SealedHeader<Client::Header>>,
) -> eyre::Result<PartialStateAdvanceOutcome>
where
    N: ProviderNodeTypes + 'static,
    Client: SnapClient + HeadersClient + BlockAccessListsClient,
{
    let tip = cached_header(client, local, target, cache).await?;
    if tip.number().saturating_sub(head.block_number) > MAX_BRANCH_HEADERS as u64 {
        return Ok(PartialStateAdvanceOutcome::TargetTooDistant)
    }
    let mut branch = Branch {
        tip: tip.num_hash(),
        anchor: None,
        headers: BTreeMap::new(),
        targets,
        factory,
        client,
    };
    let floor = head.block_number.saturating_sub(retention.max(1));
    let mut header = tip;
    loop {
        let number = header.number();
        let parent = header.parent_hash();
        if header.hash() == head.block_hash {
            eyre::ensure!(
                number == head.block_number && header.state_root() == head.state_root,
                "forkchoice ancestry disagrees with partial checkpoint"
            );
            branch.anchor = Some(*head);
            break
        }
        branch.headers.insert(number, header);
        if number <= floor {
            break
        }
        if parent == head.block_hash {
            eyre::ensure!(
                head.block_number.checked_add(1) == Some(number),
                "forkchoice branch has inconsistent parent number"
            );
            branch.anchor = Some(*head);
            break
        }
        if branch.headers.len() >= MAX_BRANCH_HEADERS {
            return Ok(PartialStateAdvanceOutcome::TargetTooDistant)
        }
        header = cached_header(client, local, parent, cache).await?;
        eyre::ensure!(
            header.number().checked_add(1) == Some(number),
            "forkchoice branch has inconsistent parent number"
        );
    }

    advance_partial_state_to_target(client, factory, &branch, filter, head, branch.tip, retention)
        .await
}

async fn cached_header<Client: HeadersClient>(
    client: &Client,
    local: &impl HeaderProvider<Header = Client::Header>,
    hash: B256,
    cache: &mut VecDeque<SealedHeader<Client::Header>>,
) -> eyre::Result<SealedHeader<Client::Header>> {
    if let Some(header) = cache.iter().rev().find(|header| header.hash() == hash) {
        return Ok(header.clone())
    }
    let header = fetch_header(client, local, hash).await?;
    // Preserve verified ancestry across cancellations, so frequent forkchoices cannot keep
    // restarting a slow catch-up download from scratch.
    if cache.len() == MAX_BRANCH_HEADERS {
        cache.pop_front();
    }
    cache.push_back(header.clone());
    Ok(header)
}

async fn fetch_header<Client: HeadersClient>(
    client: &Client,
    local: &impl HeaderProvider<Header = Client::Header>,
    hash: B256,
) -> eyre::Result<SealedHeader<Client::Header>> {
    if let Some(header) = local.header(hash)? {
        let header = SealedHeader::seal_slow(header);
        eyre::ensure!(header.hash() == hash, "local partial-state header hash mismatch");
        return Ok(header)
    }
    let response = timeout(
        REQUEST_TIMEOUT,
        client.get_headers_with_priority(HeadersRequest::one(hash.into()), Priority::High),
    )
    .await
    .map_err(|_| RequestError::Timeout)??;
    let (peer, headers) = response.split();
    if headers.is_empty() {
        return Err(DataUnavailable.into())
    }
    if headers.len() != 1 {
        client.report_bad_message(peer);
        eyre::bail!("unexpected partial-state header response length")
    }
    let header = SealedHeader::seal_slow(headers.into_iter().next().expect("length checked"));
    if header.hash() != hash {
        client.report_bad_message(peer);
        eyre::bail!("partial-state header hash mismatch")
    }
    Ok(header)
}

async fn ensure_bal<N, Client>(
    client: &Client,
    factory: &ProviderFactory<N>,
    block: BlockNumHash,
    expected: B256,
) -> eyre::Result<()>
where
    N: ProviderNodeTypes,
    Client: BlockAccessListsClient,
{
    if let Some(bal) = factory.bal_store().get_decoded_by_hash(block.hash)? {
        bal.ensure_hash(expected)?;
        return Ok(())
    }
    let response = timeout(
        REQUEST_TIMEOUT,
        client.get_block_access_lists_with_priority(vec![block.hash], Priority::High),
    )
    .await
    .map_err(|_| RequestError::Timeout)??;
    let (peer, response) = response.split();
    if response.0.is_empty() {
        return Err(DataUnavailable.into())
    }
    if response.0.len() != 1 {
        client.report_bad_message(peer);
        eyre::bail!("unexpected partial-state BAL response length")
    }
    let Some(raw) = response.0.into_iter().next().flatten() else {
        return Err(DataUnavailable.into())
    };
    let decoded = DecodedBal::from_rlp_bytes(raw.clone())
        .map_err(eyre::Report::from)
        .and_then(|bal| bal.ensure_hash(expected).map_err(eyre::Report::from));
    if let Err(err) = decoded {
        client.report_bad_message(peer);
        return Err(err.into())
    }
    factory.bal_store().insert(block, Sealed::new_unchecked(raw, expected))?;
    Ok(())
}
