use alloy_primitives::Address;
use clap::Args;
use reth_storage_api::{
    ConfiguredContractFilter, DEFAULT_PARTIAL_STATE_BAL_RETENTION, MIN_PARTIAL_STATE_BAL_RETENTION,
};
use serde::Deserialize;
use std::{collections::BTreeSet, fs, path::PathBuf};

/// Partial-state node configuration.
///
/// When enabled, a node keeps all accounts but retains contract storage and bytecode only for the
/// configured contracts. Later phases use BALs to keep tracked state up to date.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartialStateConfig {
    /// Enables partial-state mode.
    pub enabled: bool,
    /// Contracts whose storage and bytecode should be retained.
    pub contracts: BTreeSet<Address>,
    /// Optional JSON contract list path.
    pub contracts_file: Option<PathBuf>,
    /// Number of recent blocks of BAL history to retain.
    pub bal_retention: u64,
}

impl PartialStateConfig {
    /// Returns a contract filter from the configured contract set.
    pub fn contract_filter(&self) -> ConfiguredContractFilter {
        ConfiguredContractFilter::new(self.contracts.iter().copied())
    }

    /// Returns `true` if partial-state mode is enabled.
    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Returns whether the given contract is retained by this node.
    pub fn is_contract_tracked(&self, address: &Address) -> bool {
        !self.enabled || self.contracts.contains(address)
    }

    /// Returns the configured BAL retention window.
    pub const fn bal_retention(&self) -> u64 {
        self.bal_retention
    }

    /// Returns whether this configuration is the default one.
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }

    /// Validates the partial-state configuration.
    pub fn validate(&self) -> eyre::Result<()> {
        if self.enabled && self.bal_retention < MIN_PARTIAL_STATE_BAL_RETENTION {
            eyre::bail!(
                "partial-state BAL retention must be at least {} blocks, got {}",
                MIN_PARTIAL_STATE_BAL_RETENTION,
                self.bal_retention
            );
        }

        Ok(())
    }

    /// Creates a node partial-state config from the persisted TOML config.
    pub fn from_toml_config(config: reth_config::PartialStateConfig) -> eyre::Result<Self> {
        let reth_config::PartialStateConfig { enabled, contracts, contracts_file, bal_retention } =
            config;

        let mut config = Self { enabled, contracts, contracts_file, bal_retention };
        config.load_contracts_file()?;
        config.validate()?;
        Ok(config)
    }

    /// Merges this CLI-derived config with the persisted TOML config.
    ///
    /// CLI values take precedence when they are explicitly configured, while contract sets are
    /// combined so users can keep long-lived tracked contracts in `reth.toml` and add short-lived
    /// ones from the command line.
    pub fn merge_with_toml_config(
        &self,
        toml_config: &reth_config::PartialStateConfig,
    ) -> eyre::Result<Self> {
        let mut merged = Self::from_toml_config(toml_config.clone())?;

        if self.is_default() {
            return Ok(merged)
        }

        merged.enabled |= self.enabled;
        merged.contracts.extend(self.contracts.iter().copied());

        if self.contracts_file.is_some() {
            merged.contracts_file = self.contracts_file.clone();
        }

        if self.bal_retention != DEFAULT_PARTIAL_STATE_BAL_RETENTION {
            merged.bal_retention = self.bal_retention;
        }

        merged.validate()?;
        Ok(merged)
    }

    /// Loads and merges contracts from the configured contracts file, if one is set.
    pub fn load_contracts_file(&mut self) -> eyre::Result<()> {
        if let Some(path) = &self.contracts_file {
            let file = PartialStateContractsFile::read(path)?;
            self.contracts.extend(file.addresses()?);
        }

        Ok(())
    }
}

impl Default for PartialStateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            contracts: BTreeSet::new(),
            contracts_file: None,
            bal_retention: DEFAULT_PARTIAL_STATE_BAL_RETENTION,
        }
    }
}

/// CLI parameters for partial-state mode.
#[derive(Debug, Args, PartialEq, Eq, Clone)]
#[command(next_help_heading = "Partial State")]
pub struct PartialStateArgs {
    /// Enable partial-state mode.
    #[arg(long = "partial-state", default_value_t = false)]
    pub enabled: bool,
    /// Comma-separated contract addresses whose storage and bytecode should be retained.
    #[arg(long = "partial-state.contracts", value_delimiter = ',', value_name = "ADDRESS")]
    pub contracts: Vec<Address>,
    /// JSON file containing contract addresses to retain.
    #[arg(long = "partial-state.contracts-file", value_name = "PATH")]
    pub contracts_file: Option<PathBuf>,
    /// Number of recent blocks of BAL history to retain for reorg handling.
    #[arg(
        long = "partial-state.bal-retention",
        value_name = "BLOCKS",
        value_parser = clap::value_parser!(u64).range(MIN_PARTIAL_STATE_BAL_RETENTION..),
        default_value_t = DEFAULT_PARTIAL_STATE_BAL_RETENTION
    )]
    pub bal_retention: u64,
}

impl PartialStateArgs {
    /// Converts CLI args into validated partial-state configuration.
    pub fn into_config(self) -> eyre::Result<PartialStateConfig> {
        let Self { enabled, contracts, contracts_file, bal_retention } = self;

        let contracts: BTreeSet<_> = contracts.into_iter().collect();

        let mut config = PartialStateConfig { enabled, contracts, contracts_file, bal_retention };
        config.load_contracts_file()?;
        config.validate()?;
        Ok(config)
    }
}

impl Default for PartialStateArgs {
    fn default() -> Self {
        Self {
            enabled: false,
            contracts: Vec::new(),
            contracts_file: None,
            bal_retention: DEFAULT_PARTIAL_STATE_BAL_RETENTION,
        }
    }
}

#[derive(Debug, Deserialize)]
struct PartialStateContractsFile {
    version: u64,
    contracts: Vec<PartialStateContractEntry>,
}

impl PartialStateContractsFile {
    fn read(path: &PathBuf) -> eyre::Result<Self> {
        let data = fs::read_to_string(path)
            .map_err(|err| eyre::eyre!("failed to read partial-state contracts file: {err}"))?;
        let file = serde_json::from_str::<Self>(&data)
            .map_err(|err| eyre::eyre!("failed to parse partial-state contracts file: {err}"))?;

        if file.version != 1 {
            eyre::bail!("unsupported partial-state contracts file version: {}", file.version);
        }

        Ok(file)
    }

    fn addresses(self) -> eyre::Result<Vec<Address>> {
        self.contracts
            .into_iter()
            .map(|entry| {
                entry.address.parse::<Address>().map_err(|err| {
                    eyre::eyre!(
                        "invalid contract address in partial-state contracts file: {} ({err})",
                        entry.address
                    )
                })
            })
            .collect()
    }
}

#[derive(Debug, Deserialize)]
struct PartialStateContractEntry {
    address: String,
    #[expect(dead_code)]
    name: Option<String>,
    #[expect(dead_code)]
    comment: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;
    use clap::Parser;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[derive(Parser)]
    struct CommandParser<T: Args> {
        #[command(flatten)]
        args: T,
    }

    #[test]
    fn default_partial_state_args_are_disabled() {
        let args = CommandParser::<PartialStateArgs>::parse_from(["reth"]).args;

        assert!(!args.enabled);
        assert!(args.contracts.is_empty());
        assert_eq!(args.bal_retention, DEFAULT_PARTIAL_STATE_BAL_RETENTION);
    }

    #[test]
    fn parses_partial_state_contracts() {
        let args = CommandParser::<PartialStateArgs>::parse_from([
            "reth",
            "--partial-state",
            "--partial-state.contracts",
            "0x0000000000000000000000000000000000000001,0x0000000000000000000000000000000000000002",
            "--partial-state.bal-retention",
            "64",
        ])
        .args;

        let config = args.into_config().unwrap();

        assert!(config.enabled);
        assert_eq!(config.contracts.len(), 2);
        assert_eq!(config.bal_retention, 64);
    }

    #[test]
    fn loads_contracts_file_and_deduplicates_inline_contracts() {
        let inline = address!("0000000000000000000000000000000000000001");
        let file_only = address!("0000000000000000000000000000000000000002");
        let path = std::env::temp_dir().join(format!(
            "reth-partial-state-contracts-{}-{}.json",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ));
        fs::write(
            &path,
            r#"{
                "version": 1,
                "contracts": [
                    {
                        "address": "0x0000000000000000000000000000000000000001",
                        "name": "duplicate inline contract"
                    },
                    {
                        "address": "0x0000000000000000000000000000000000000002",
                        "comment": "file-only contract"
                    }
                ]
            }"#,
        )
        .unwrap();

        let args = PartialStateArgs {
            enabled: true,
            contracts: vec![inline],
            contracts_file: Some(path.clone()),
            bal_retention: MIN_PARTIAL_STATE_BAL_RETENTION,
        };

        let config = args.into_config().unwrap();
        let _ = fs::remove_file(&path);

        assert_eq!(config.contracts, BTreeSet::from([inline, file_only]));
        assert_eq!(config.contracts_file, Some(path));
    }

    #[test]
    fn merges_cli_config_with_toml_config() {
        let from_cli = address!("0000000000000000000000000000000000000001");
        let from_toml = address!("0000000000000000000000000000000000000002");
        let cli_config = PartialStateConfig {
            enabled: true,
            contracts: BTreeSet::from([from_cli]),
            contracts_file: None,
            bal_retention: MIN_PARTIAL_STATE_BAL_RETENTION,
        };
        let toml_config = reth_config::PartialStateConfig {
            enabled: true,
            contracts: BTreeSet::from([from_toml]),
            contracts_file: None,
            bal_retention: 128,
        };

        let merged = cli_config.merge_with_toml_config(&toml_config).unwrap();

        assert!(merged.is_enabled());
        assert_eq!(merged.contracts, BTreeSet::from([from_cli, from_toml]));
        assert_eq!(merged.bal_retention(), MIN_PARTIAL_STATE_BAL_RETENTION);
        assert!(merged.is_contract_tracked(&from_cli));
        assert!(merged.is_contract_tracked(&from_toml));
    }

    #[test]
    fn uses_toml_config_when_cli_config_is_default() {
        let tracked = address!("0000000000000000000000000000000000000002");
        let cli_config = PartialStateConfig::default();
        let toml_config = reth_config::PartialStateConfig {
            enabled: true,
            contracts: BTreeSet::from([tracked]),
            contracts_file: None,
            bal_retention: 128,
        };

        let merged = cli_config.merge_with_toml_config(&toml_config).unwrap();

        assert!(merged.is_enabled());
        assert_eq!(merged.contracts, BTreeSet::from([tracked]));
        assert_eq!(merged.bal_retention(), 128);
        assert!(merged.is_contract_tracked(&tracked));
    }

    #[test]
    fn treats_every_contract_as_tracked_when_disabled() {
        let address = address!("0000000000000000000000000000000000000001");

        assert!(PartialStateConfig::default().is_contract_tracked(&address));
    }

    #[test]
    fn rejects_too_low_bal_retention_when_enabled() {
        let args = PartialStateArgs {
            enabled: true,
            bal_retention: MIN_PARTIAL_STATE_BAL_RETENTION - 1,
            ..Default::default()
        };

        assert!(args.into_config().is_err());
    }
}
