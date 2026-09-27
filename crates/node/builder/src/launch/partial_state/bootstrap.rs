//! Explicit trusted bootstrap, independent of executed headers and full-state persistence.

use super::{forkchoice, wait_for_partial_state_bootstrap, PartialStateBootstrap};
use alloy_consensus::BlockHeader;
use reth_chainspec::{ChainSpecProvider, EthChainSpec, EthereumHardforks};
use reth_config::PartialStateTrustedCheckpoint;
use reth_network_p2p::headers::client::HeadersClient;
use reth_provider::{providers::ProviderNodeTypes, HeaderProvider, ProviderFactory};
use reth_storage_api::{
    ConfiguredContractFilter, PartialStateCheckpointProvider, PartialStateRootProvider,
    PartialStateSnapPivot,
};
use reth_tracing::tracing::{info, warn};
use std::time::Duration;

/// Keeps legacy pivot selection unless an operator explicitly supplies a trust anchor.
pub(crate) async fn wait_for_bootstrap<N, Client>(
    client: &Client,
    factory: &ProviderFactory<N>,
    local: &impl HeaderProvider,
    filter: &ConfiguredContractFilter,
    resume: bool,
    trusted: Option<PartialStateTrustedCheckpoint>,
) -> eyre::Result<PartialStateBootstrap>
where
    N: ProviderNodeTypes + 'static,
    Client: HeadersClient,
{
    let Some(trusted) = trusted else {
        return wait_for_partial_state_bootstrap(factory, local, filter, resume).await
    };
    loop {
        match select_trusted_bootstrap(client, factory, filter, resume, trusted).await {
            Err(err) if forkchoice::unavailable(&err) => {
                warn!(target: "reth::cli", block_hash = %trusted.block_hash, %err,
                    "Partial-state trusted bootstrap deferred; waiting for peer header availability");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            result => return result,
        }
    }
}

pub(super) async fn select_trusted_bootstrap<N, Client>(
    client: &Client,
    factory: &ProviderFactory<N>,
    filter: &ConfiguredContractFilter,
    resume: bool,
    trusted: PartialStateTrustedCheckpoint,
) -> eyre::Result<PartialStateBootstrap>
where
    N: ProviderNodeTypes + 'static,
    Client: HeadersClient,
{
    trusted.validate()?;
    let spec = factory.chain_spec();
    eyre::ensure!(
        trusted.chain_id == spec.chain().id() && trusted.genesis_hash == spec.genesis_hash(),
        "partial-state trusted checkpoint belongs to a different chain"
    );
    // A complete checkpoint was verified by this node. The configured seed is used for fresh
    // downloads, not as a constraint that would prevent resuming later blocks or a reorg branch.
    let saved = factory.partial_state_checkpoint()?;
    let saved = saved.filter(|checkpoint| {
        resume &&
            checkpoint.is_complete_for(filter) &&
            checkpoint.pivot.block_number >= trusted.block_number
    });
    let pivot = saved.map_or(
        PartialStateSnapPivot {
            block_number: trusted.block_number,
            block_hash: trusted.block_hash,
            state_root: trusted.state_root,
        },
        |checkpoint| checkpoint.pivot,
    );
    let header = forkchoice::fetch_peer_header(client, pivot.block_hash).await?;
    eyre::ensure!(
        header.number() == pivot.block_number && header.state_root() == pivot.state_root,
        "partial-state trusted checkpoint does not match its peer header"
    );
    // Unlike the legacy path there may be no canonical child locally. Require activation at
    // the seed itself rather than assuming that an arbitrary pre-fork state can be replayed.
    eyre::ensure!(
        spec.is_amsterdam_active_at_timestamp(header.timestamp()),
        "partial-state trusted checkpoint must be at or after BAL activation"
    );
    eyre::ensure!(
        header.number() == 0 || header.block_access_list_hash().is_some(),
        "BAL-active partial-state trusted checkpoint is missing its BAL commitment"
    );
    let bootstrap = if saved.is_some() {
        let factory = factory.clone();
        let filter = filter.clone();
        let root =
            tokio::task::spawn_blocking(move || factory.partial_state_root(&filter)).await??;
        eyre::ensure!(root == pivot.state_root, "persisted partial-state checkpoint root mismatch");
        PartialStateBootstrap::Resume(pivot)
    } else {
        PartialStateBootstrap::Sync(pivot)
    };
    info!(target: "reth::cli", block_number = pivot.block_number,
        block_hash = %pivot.block_hash, state_root = %pivot.state_root,
        resume = saved.is_some(), "Selected peer-backed partial-state bootstrap");
    Ok(bootstrap)
}
