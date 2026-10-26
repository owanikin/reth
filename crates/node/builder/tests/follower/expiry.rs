//! Exercise explicit expiry through a running follower, including its warmed HTTP RPC path.

use super::*;
use reth_chainspec::ChainSpecProvider;
use reth_provider::{
    providers::{BlockchainProvider, ProviderNodeTypes},
    ProviderFactory, RocksDBProviderFactory, StaticFileProviderFactory,
};
use reth_storage_api::PartialStateSnapPivot;

#[test]
fn partial_state_follower_storage_expiry_survives_restart_and_reorg() {
    assert_restart(2, true);
}

pub(super) async fn expire<N: ProviderNodeTypes>(
    db: N::DB,
    provider: &BlockchainProvider<N>,
    block: &BranchBlock,
) {
    // Share the node's database and auxiliary stores; do not open a second MDBX environment.
    let factory = ProviderFactory::<N>::new(
        db,
        provider.chain_spec(),
        provider.static_file_provider(),
        provider.rocksdb_provider(),
        reth_tasks::Runtime::test(),
    )
    .unwrap();
    let pivot = PartialStateSnapPivot {
        block_number: block.header.number,
        block_hash: block.header.hash_slow(),
        state_root: block.header.state_root,
    };
    let outcome = tokio::task::spawn_blocking(move || {
        factory.expire_partial_state_storage(
            pivot,
            &ConfiguredContractFilter::new([TRACKED]),
            &[TRACKED],
        )
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(outcome.accounts_expired, 1);
    assert_eq!(outcome.slots_deleted, 2);
    assert_eq!(outcome.journal_slots_deleted, 4);
}

pub(super) fn assert_only_storage_removed(before: &Snapshot, after: &Snapshot) {
    assert_eq!(after.full, before.full, "expiry modified normal state");
    for (table, rows) in before.partial.as_object().unwrap() {
        if !matches!(
            table.as_str(),
            "storage" | "storage_changes" | "availability" | "availability_changes"
        ) {
            assert_eq!(&after.partial[table], rows, "expiry modified {table}");
        }
    }
}

pub(super) fn assert_persisted(db: &impl Database, block: &BranchBlock) {
    let tx = db.tx().unwrap();
    let hash = keccak256(TRACKED);
    let availability = tx.get::<tables::PartialStateAvailability>(hash).unwrap().unwrap();
    assert!(!availability.storage);
    assert!(availability.bytecode, "storage expiry must not expire bytecode");
    assert!(rows::<tables::PartialStateStorages>(&tx).is_empty(), "expired slots reappeared");
    assert!(
        rows::<tables::PartialStateStorageChangeSets>(&tx).is_empty(),
        "expired slot before-images reappeared"
    );
    for (key, availability) in rows::<tables::PartialStateAvailabilityChangeSets>(&tx) {
        if key.address() == TRACKED {
            assert!(!availability.storage, "reorg could restore expired storage");
            assert!(availability.bytecode);
        }
    }
    assert_eq!(
        tx.get::<tables::PartialStateAccounts>(hash).unwrap(),
        Some(Account::from(block.accounts()[0].1))
    );
    assert_eq!(
        tx.get::<tables::PartialStateStorageRoots>(hash).unwrap(),
        Some(block.accounts()[0].1.storage_root)
    );
}

pub(super) async fn assert_rpc(client: &HttpClient, block: &BranchBlock) {
    for at in [
        json!("latest"),
        json!(format!("0x{:x}", block.header.number)),
        json!({"blockHash": block.header.hash_slow()}),
    ] {
        // Even a previously absent slot cannot be assumed zero after its trie is expired.
        for slot in [0, 1, 99] {
            assert_storage_error(
                client,
                "eth_getStorageAt",
                rpc_params![TRACKED, U256::from(slot), at.clone()],
            )
            .await;
        }
        for method in ["eth_call", "eth_estimateGas"] {
            assert_storage_error(
                client,
                method,
                rpc_params![json!({"to": TRACKED, "gasPrice": "0x0"}), at.clone()],
            )
            .await;
        }
        let code: Bytes =
            client.request("eth_getCode", rpc_params![TRACKED, at.clone()]).await.unwrap();
        assert_eq!(code.as_ref(), CODE);
        let balance: U256 =
            client.request("eth_getBalance", rpc_params![TRACKED, at.clone()]).await.unwrap();
        assert_eq!(balance, U256::from(100));
        let nonce: U256 = client
            .request("eth_getTransactionCount", rpc_params![TRACKED, at.clone()])
            .await
            .unwrap();
        assert_eq!(nonce, U256::from(1));

        for (method, params, code) in [
            ("eth_getCode", rpc_params![UNTRACKED, at.clone()], -32002),
            ("eth_getStorageAt", rpc_params![UNTRACKED, "0x0", at], -32001),
        ] {
            let err = client.request::<Value, _>(method, params).await.unwrap_err();
            let jsonrpsee::core::client::Error::Call(err) = err else { panic!("{err}") };
            assert_eq!(err.code(), code, "untracked and expired errors must stay distinct");
            assert!(err.message().contains("not tracked"));
        }
    }
    let executed: U256 = client.request("eth_blockNumber", rpc_params![]).await.unwrap();
    assert_eq!(executed, U256::ZERO);
}

async fn assert_storage_error(
    client: &HttpClient,
    method: &str,
    params: jsonrpsee::core::params::ArrayParams,
) {
    let err = client.request::<Value, _>(method, params).await.unwrap_err();
    let jsonrpsee::core::client::Error::Call(err) = err else { panic!("{method}: {err}") };
    assert_eq!(err.code(), -32003, "{method}: {err}");
    let message = err.message().to_lowercase();
    assert!(message.contains("storage"), "{method}: {err}");
    assert!(message.contains("unavailable"), "{method}: {err}");
    assert!(message.contains(&TRACKED.to_string().to_lowercase()), "{method}: {err}");
}
