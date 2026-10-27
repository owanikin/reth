use super::*;
use alloy_consensus::Header;
use alloy_primitives::{keccak256, U256};
use reth_chainspec::MAINNET;
use reth_db::init_db;
use reth_db_api::{table::Table, transaction::DbTxMut, Database};
use reth_ethereum_cli::chainspec::EthereumChainSpecParser;
use reth_primitives_traits::Account;
use reth_provider::{
    providers::StaticFileProvider,
    test_utils::{MockNodeTypes, MockNodeTypesWithDB},
};
use reth_stages_types::StageCheckpoint;
use reth_storage_api::{DBProvider, PartialStateSnapPivot, PartialStateSnapWriter};
use reth_trie::root::{state_root_unsorted, storage_root_unsorted};
use std::process::Command as ProcessCommand;

const TRACKED: Address = Address::repeat_byte(1);

struct Fixture {
    dir: tempfile::TempDir,
    pivot: PartialStateSnapPivot,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let sf_path = dir.path().join("static_files");
        reth_fs_util::create_dir_all(&sf_path).unwrap();
        let factory = ProviderFactory::<MockNodeTypesWithDB<DatabaseEnv>>::new(
            init_db(dir.path().join("db"), DatabaseArguments::default()).unwrap(),
            MAINNET.clone(),
            StaticFileProvider::read_write(sf_path).unwrap(),
            RocksDBProvider::builder(dir.path().join("rocksdb"))
                .with_default_tables()
                .build()
                .unwrap(),
            reth_tasks::Runtime::test(),
        )
        .unwrap();
        let filter = ConfiguredContractFilter::new([TRACKED]);
        let key = B256::repeat_byte(7);
        let storage_root = storage_root_unsorted([(key, U256::from(5))]);
        let account = Account::default().into_trie_account(storage_root);
        let root = state_root_unsorted([(keccak256(TRACKED), account)]);
        let header = Header { number: 12, state_root: root, ..Default::default() };
        let pivot = PartialStateSnapPivot {
            block_number: header.number,
            block_hash: header.hash_slow(),
            state_root: root,
        };
        factory.begin_partial_state_sync(pivot, &filter).unwrap();
        let provider = factory.database_provider_rw().unwrap();
        reth_db_common::init::insert_genesis_header(&provider, MAINNET.as_ref()).unwrap();
        let mut writer = provider.partial_state_snap_writer();
        writer.write_account(keccak256(TRACKED), account).unwrap();
        writer.write_storage(keccak256(TRACKED), key, U256::from(5)).unwrap();
        provider.tx_ref().put::<tables::PartialStateHeaders<Header>>(12, header).unwrap();
        provider.tx_ref().put::<tables::CanonicalHeaders>(0, MAINNET.genesis_hash()).unwrap();
        provider
            .tx_ref()
            .put::<tables::StageCheckpoints>("Finish".to_string(), StageCheckpoint::new(0))
            .unwrap();
        provider.commit().unwrap();
        factory.complete_partial_state_sync(pivot, &filter).unwrap();
        drop(factory);

        let mut config = Config::default();
        config.partial_state.enabled = true;
        config.partial_state.follower = true;
        config.partial_state.contracts.insert(TRACKED);
        config.partial_state.trusted_checkpoint =
            Some(reth_config::PartialStateTrustedCheckpoint {
                chain_id: 1,
                genesis_hash: MAINNET.genesis_hash(),
                block_number: pivot.block_number,
                block_hash: pivot.block_hash,
                state_root: pivot.state_root,
            });
        reth_fs_util::write(dir.path().join("reth.toml"), toml::to_string(&config).unwrap())
            .unwrap();
        Self { dir, pivot }
    }

    fn env(&self) -> EnvironmentArgs<EthereumChainSpecParser> {
        EnvironmentArgs::parse_from(["reth", "--datadir", self.dir.path().to_str().unwrap()])
    }

    fn command(&self, apply: bool) -> Command {
        Command {
            address: vec![TRACKED, TRACKED],
            apply,
            expected_block_hash: Some(self.pivot.block_hash),
        }
    }

    fn run(&self, command: Command) -> eyre::Result<ExpiryReport> {
        command.execute::<EthereumChainSpecParser, MockNodeTypes>(
            &self.env(),
            reth_tasks::Runtime::test(),
        )
    }

    fn update<T: Table>(&self, key: T::Key, value: T::Value) {
        let db =
            open_offline_database(&self.dir.path().join("db"), DatabaseArguments::default(), true)
                .unwrap();
        let tx = db.tx_mut().unwrap();
        tx.put::<T>(key, value).unwrap();
        tx.commit().unwrap();
    }

    fn snapshot(&self) -> String {
        use reth_db_api::cursor::DbCursorRO;
        let db =
            open_offline_database(&self.dir.path().join("db"), DatabaseArguments::default(), false)
                .unwrap();
        let tx = db.tx().unwrap();
        let mut snapshot = String::new();
        macro_rules! rows {
            ($($table:ty),+ $(,)?) => {$(
                snapshot.push_str(&format!("{:?}", tx.cursor_read::<$table>().unwrap().walk(None).unwrap().collect::<Result<Vec<_>, _>>().unwrap()));
            )+};
        }
        rows!(
            tables::PartialStateAccounts,
            tables::PartialStateStorages,
            tables::PartialStateStorageRoots,
            tables::PartialStateAvailability,
            tables::PartialStateCheckpoints,
            tables::PartialStateHeaders<Header>,
            tables::StageCheckpoints,
            tables::CanonicalHeaders
        );
        snapshot
    }
}

#[test]
fn partial_state_expire_parse_requires_explicit_apply_and_hash() {
    let address = TRACKED.to_string();
    let cmd = Command::try_parse_from(["expire", "--address", &address]).unwrap();
    assert!(!cmd.apply);
    assert!(cmd.expected_block_hash.is_none());
    assert!(Command::try_parse_from(["expire"]).is_err());
    assert!(Command::try_parse_from(["expire", "--address", "bad"]).is_err());
    assert!(Command::try_parse_from(["expire", "--address", &address, "--apply"]).is_err());
    assert!(Command::try_parse_from([
        "expire",
        "--address",
        &address,
        "--apply",
        "--expected-block-hash",
        &B256::ZERO.to_string()
    ])
    .is_ok());
}

#[test]
fn partial_state_expire_preview_apply_and_repeat() {
    let f = Fixture::new();
    let before = f.snapshot();
    let preview = f.run(f.command(false)).unwrap();
    assert!(!preview.applied);
    assert_eq!(preview.slots_deleted, 1);
    assert_eq!(preview.addresses, vec![TRACKED]);
    assert_eq!(before, f.snapshot());
    let applied = f.run(f.command(true)).unwrap();
    assert!(applied.applied);
    assert_eq!(applied.accounts_expired, preview.accounts_expired);
    assert_eq!(applied.slots_deleted, preview.slots_deleted);
    assert_eq!(applied.state_root, preview.state_root);
    let after = f.snapshot();
    let repeated = f.run(f.command(true)).unwrap();
    assert_eq!(repeated.slots_deleted, 0);
    assert_eq!(repeated.accounts_expired, 0);
    assert_eq!(after, f.snapshot());
}

#[test]
fn partial_state_expire_rejects_stale_hash_untracked_target_and_missing_confirmation() {
    let f = Fixture::new();
    let before = f.snapshot();
    for apply in [false, true] {
        let mut cmd = f.command(apply);
        cmd.expected_block_hash = Some(B256::ZERO);
        assert!(f.run(cmd).unwrap_err().to_string().contains("Checkpoint changed"));
        let mut cmd = f.command(apply);
        cmd.address.push(Address::repeat_byte(2));
        assert!(f.run(cmd).is_err());
    }
    let mut cmd = f.command(true);
    cmd.expected_block_hash = None;
    assert!(f.run(cmd).is_err());
    assert_eq!(before, f.snapshot());
}

#[test]
fn partial_state_expire_rejects_wrong_genesis_finish_and_header() {
    let f = Fixture::new();
    let config_path = f.dir.path().join("reth.toml");
    let original = reth_fs_util::read_to_string(&config_path).unwrap();
    let mut config: Config = toml::from_str(&original).unwrap();
    let seed = config.partial_state.trusted_checkpoint.as_mut().unwrap();
    seed.chain_id = reth_chainspec::SEPOLIA.chain().id();
    seed.genesis_hash = reth_chainspec::SEPOLIA.genesis_hash();
    reth_fs_util::write(&config_path, toml::to_string(&config).unwrap()).unwrap();
    let mut env = f.env();
    env.chain = reth_chainspec::SEPOLIA.clone();
    assert!(f
        .command(true)
        .execute::<EthereumChainSpecParser, MockNodeTypes>(&env, reth_tasks::Runtime::test(),)
        .unwrap_err()
        .to_string()
        .contains("Database genesis"));
    reth_fs_util::write(&config_path, original).unwrap();
    f.update::<tables::StageCheckpoints>("Finish".to_string(), StageCheckpoint::new(1));
    assert!(f.run(f.command(true)).unwrap_err().to_string().contains("Normal execution"));
    f.update::<tables::StageCheckpoints>("Finish".to_string(), StageCheckpoint::new(0));
    f.update::<tables::PartialStateHeaders<Header>>(12, Header::default());
    let before = f.snapshot();
    assert!(f.run(f.command(true)).unwrap_err().to_string().contains("saved header"));
    assert_eq!(before, f.snapshot());
}

#[test]
fn partial_state_expire_rejects_incomplete_filter_mismatch_and_corrupt_root() {
    let f = Fixture::new();
    let db = open_offline_database(&f.dir.path().join("db"), DatabaseArguments::default(), false)
        .unwrap();
    let original = db.tx().unwrap().get::<tables::PartialStateCheckpoints>(0).unwrap().unwrap();
    drop(db);
    let mut incomplete = original;
    incomplete.sync_complete = false;
    let mut changed_filter = original;
    changed_filter.filter_hash = B256::ZERO;
    for checkpoint in [incomplete, changed_filter] {
        f.update::<tables::PartialStateCheckpoints>(0, checkpoint);
        let before = f.snapshot();
        for apply in [false, true] {
            assert!(f.run(f.command(apply)).is_err());
        }
        assert_eq!(before, f.snapshot());
    }
    f.update::<tables::PartialStateCheckpoints>(0, original);
    f.update::<tables::PartialStateAccounts>(
        keccak256(TRACKED),
        Account { nonce: 1, ..Default::default() },
    );
    let before = f.snapshot();
    for apply in [false, true] {
        assert!(f.run(f.command(apply)).is_err());
    }
    assert_eq!(before, f.snapshot());
}

#[test]
fn partial_state_expire_never_initializes_missing_storage() {
    let dir = tempfile::tempdir().unwrap();
    for apply in [false, true] {
        assert!(open_offline_database(dir.path(), DatabaseArguments::default(), apply).is_err());
    }
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn partial_state_expire_rejects_missing_malformed_and_nonfollower_config() {
    let f = Fixture::new();
    let path = f.dir.path().join("reth.toml");
    let before = f.snapshot();
    for text in ["bad = [", "", "[partial_state]\nenabled = true"] {
        reth_fs_util::write(&path, text).unwrap();
        assert!(f.run(f.command(true)).is_err());
    }
    reth_fs_util::remove_file(&path).unwrap();
    assert!(f.run(f.command(true)).is_err());
    assert!(!path.exists());
    assert_eq!(before, f.snapshot());
}

#[test]
fn partial_state_expire_refuses_open_database_in_another_process() {
    let f = Fixture::new();
    let path = f.dir.path().join("db");
    // A read-only holder deliberately has no PID lock; only MDBX exclusivity can reject it.
    let db = reth_db::open_db_read_only(&path, DatabaseArguments::default()).unwrap();
    let status = ProcessCommand::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "db::partial_state_expire::tests::partial_state_expire_lock_probe",
            "--nocapture",
        ])
        .env("RETH_EXPIRY_LOCK_TEST", &path)
        .status()
        .unwrap();
    assert!(status.success());
    drop(db);
    assert!(open_offline_database(&path, DatabaseArguments::default(), false).is_ok());
}

#[test]
fn partial_state_expire_lock_probe() {
    let Some(path) = std::env::var_os("RETH_EXPIRY_LOCK_TEST") else { return };
    for apply in [false, true] {
        assert!(
            open_offline_database(Path::new(&path), DatabaseArguments::default(), apply).is_err()
        );
    }
}
