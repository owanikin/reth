use crate::common::EnvironmentArgs;
use alloy_consensus::BlockHeader;
use alloy_primitives::{Address, Sealable, B256};
use clap::Parser;
use eyre::{ensure, Context, ContextCompat};
use reth_chainspec::EthChainSpec;
use reth_cli::chainspec::ChainSpecParser;
use reth_config::Config;
use reth_db::{
    mdbx::DatabaseArguments, version::check_db_version_file, DatabaseEnv, DatabaseEnvKind,
};
use reth_db_api::{tables, transaction::DbTx};
use reth_node_builder::NodeTypesWithDBAdapter;
use reth_node_core::args::PartialStateConfig;
use reth_primitives_traits::NodePrimitives;
use reth_provider::{
    providers::{
        NodeTypesForProvider, ProviderNodeTypes, RocksDBProvider, StaticFileProviderBuilder,
    },
    ProviderFactory,
};
use reth_stages_types::StageId;
use reth_storage_api::{
    BlockHashReader, ConfiguredContractFilter, DatabaseProviderFactory,
    PartialStateCheckpointProvider, StageCheckpointReader,
};
use serde::Serialize;
use std::{collections::BTreeSet, path::Path};

/// Explicit storage expiry for an offline, non-validating partial-state follower.
///
/// Stop the follower and all other processes using its database first. Reads the complete
/// tracking filter from --config (or the datadir's reth.toml). Preview is the default.
/// Applying expiry deletes selected slots and their rollback history, but preserves account
/// commitments, bytecode and the verified checkpoint. This does not shrink the database file.
#[derive(Debug, Parser)]
pub struct Command {
    /// Tracked contracts whose storage should expire (repeatable or comma-separated).
    #[arg(long, required = true, value_delimiter = ',', value_name = "ADDRESS")]
    address: Vec<Address>,

    /// Apply expiry. Without this flag, only validate and report the rows to be deleted.
    #[arg(long)]
    apply: bool,

    /// Checkpoint block hash from the preview; required when applying expiry.
    #[arg(long, value_name = "HASH", required_if_eq("apply", "true"))]
    expected_block_hash: Option<B256>,
}

impl Command {
    pub(super) fn execute<C, N>(
        self,
        env: &EnvironmentArgs<C>,
        runtime: reth_tasks::Runtime,
    ) -> eyre::Result<ExpiryReport>
    where
        C: ChainSpecParser<ChainSpec: EthChainSpec>,
        N: NodeTypesForProvider<ChainSpec = C::ChainSpec>,
    {
        ensure!(!self.address.is_empty(), "Select at least one tracked address");
        ensure!(
            !self.apply || self.expected_block_hash.is_some(),
            "--apply requires --expected-block-hash from a preview"
        );
        let data_dir = env.datadir.clone().resolve_datadir(env.chain.chain());
        let config_path = env.config.clone().unwrap_or_else(|| data_dir.config());
        // Config::from_path initializes a missing file. An offline destructive command must
        // fail closed instead, and must never infer the filter from just the expiry targets.
        let config: Config = toml::from_str(&reth_fs_util::read_to_string(&config_path)?)
            .wrap_err("Invalid follower configuration")?;
        let partial = PartialStateConfig::from_toml_config(config.partial_state)?;
        ensure!(partial.enabled && partial.follower, "Storage expiry requires follower = true");
        let seed = partial.trusted_checkpoint.context("Missing trusted checkpoint")?;
        ensure!(
            seed.chain_id == env.chain.chain().id() &&
                seed.genesis_hash == env.chain.genesis_hash(),
            "Follower configuration does not match --chain"
        );
        let filter = partial.contract_filter();
        let db_path = data_dir.db();
        let sf_path = data_dir.static_files();
        let rocks_path = data_dir.rocksdb();
        ensure!(sf_path.is_dir(), "Static files directory does not exist: {sf_path:?}");
        ensure!(RocksDBProvider::exists(&rocks_path), "RocksDB does not exist: {rocks_path:?}");

        // Do not use EnvironmentArgs::init: it can initialize genesis, migrate tables, or
        // unwind storage. Only MDBX needs write access, and only when explicitly applying.
        let db = open_offline_database(&db_path, env.db.database_args(), self.apply)?;
        let genesis_number = env.chain.genesis().number.unwrap_or_default();
        let factory = ProviderFactory::<NodeTypesWithDBAdapter<N, DatabaseEnv>>::new(
            db,
            env.chain.clone(),
            StaticFileProviderBuilder::read_only(sf_path)
                .with_genesis_block_number(genesis_number)
                .build()?,
            RocksDBProvider::builder(rocks_path)
                .with_default_tables()
                .with_read_only(true)
                .build()?,
            runtime,
        )?;
        ensure!(
            factory.block_hash(genesis_number)? == Some(env.chain.genesis_hash()),
            "Database genesis does not match --chain"
        );
        let provider = factory.database_provider_ro()?;
        let finish =
            provider.get_stage_checkpoint(StageId::Finish)?.context("Missing Finish checkpoint")?;
        ensure!(
            finish.block_number == genesis_number,
            "Normal execution has advanced; this command only supports follower databases"
        );
        drop(provider);
        self.run(&factory, &filter)
    }

    fn run<N: ProviderNodeTypes>(
        &self,
        factory: &ProviderFactory<N>,
        filter: &ConfiguredContractFilter,
    ) -> eyre::Result<ExpiryReport> {
        let checkpoint =
            factory.partial_state_checkpoint()?.context("Missing partial checkpoint")?;
        let pivot = checkpoint.pivot;
        ensure!(
            checkpoint.is_complete_for(filter),
            "Incomplete checkpoint or tracking filter mismatch"
        );
        ensure!(
            self.expected_block_hash.is_none_or(|hash| hash == pivot.block_hash),
            "Checkpoint changed since preview; preview again before applying expiry"
        );
        let provider = factory.database_provider_ro()?;
        let header = provider
            .tx_ref()
            .get::<tables::PartialStateHeaders<<N::Primitives as NodePrimitives>::BlockHeader>>(
                pivot.block_number,
            )?
            .context("Missing partial checkpoint header")?;
        ensure!(
            header.number() == pivot.block_number &&
                header.hash_slow() == pivot.block_hash &&
                header.state_root() == pivot.state_root,
            "Partial checkpoint does not match its saved header"
        );
        drop(provider);
        let addresses: Vec<_> =
            self.address.iter().copied().collect::<BTreeSet<_>>().into_iter().collect();
        let outcome = if self.apply {
            factory.expire_partial_state_storage(pivot, filter, &addresses)?
        } else {
            factory.preview_partial_state_storage_expiry(pivot, filter, &addresses)?
        };
        Ok(ExpiryReport {
            applied: self.apply,
            block_number: pivot.block_number,
            block_hash: pivot.block_hash,
            state_root: pivot.state_root,
            filter_hash: checkpoint.filter_hash,
            addresses,
            accounts_expired: outcome.accounts_expired,
            slots_deleted: outcome.slots_deleted,
            journal_slots_deleted: outcome.journal_slots_deleted,
        })
    }
}

#[derive(Debug, Serialize)]
pub(super) struct ExpiryReport {
    applied: bool,
    block_number: u64,
    block_hash: B256,
    state_root: B256,
    filter_hash: B256,
    addresses: Vec<Address>,
    accounts_expired: usize,
    slots_deleted: usize,
    journal_slots_deleted: usize,
}

fn open_offline_database(
    path: &Path,
    args: DatabaseArguments,
    apply: bool,
) -> eyre::Result<DatabaseEnv> {
    ensure!(path.join("mdbx.dat").is_file(), "Existing MDBX database required: {path:?}");
    check_db_version_file(path)?;
    // MDBX's OS-level exclusive lock also protects against nodes in other PID namespaces;
    // a PID lock alone is insufficient for a Docker-mounted database.
    DatabaseEnv::open(
        path,
        if apply { DatabaseEnvKind::RW } else { DatabaseEnvKind::RO },
        args.with_exclusive(Some(true)),
    )
    .wrap_err(
        "Cannot open offline database exclusively; stop the follower and other database users",
    )
}

#[cfg(test)]
mod tests;
