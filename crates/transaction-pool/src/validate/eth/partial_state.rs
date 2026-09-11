use super::*;
use crate::{
    blobstore::InMemoryBlobStore, error::PoolErrorKind, CoinbaseTipOrdering, EthPooledTransaction,
    Pool, PoolTransaction, TransactionPool,
};
use alloy_consensus::{Header, TxLegacy};
use alloy_primitives::{keccak256, Address, Bytes, Signature, TxKind, B256};
use reth_chain_state::{ExecutedBlock, NewCanonicalChain};
use reth_chainspec::DEV;
use reth_db_api::{
    models::StoredPartialStateCheckpoint,
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_ethereum_primitives::{Block, Transaction, TransactionSigned};
use reth_evm_ethereum::EthEvmConfig;
use reth_primitives_traits::{Block as _, Bytecode, Recovered, SealedHeader};
use reth_provider::{
    providers::BlockchainProvider,
    test_utils::{create_test_provider_factory_with_chain_spec, MockNodeTypesWithDB},
    ProviderFactory,
};
use reth_storage_api::{
    errors::provider::ProviderError, BlockWriter, CanonChainTracker, ContractFilter, DBProvider,
    DatabaseProviderFactory, PartialStateRootProvider,
};

const SENDER: Address = Address::repeat_byte(0x11);
const TRACKED: Address = Address::repeat_byte(0x22);
const UNTRACKED: Address = Address::repeat_byte(0x33);
const RECIPIENT: Address = Address::repeat_byte(0x44);
const BALANCE: u64 = 1_000_000_000_000_000_000;

type Validator = EthTransactionValidator<
    BlockchainProvider<MockNodeTypesWithDB>,
    EthPooledTransaction,
    EthEvmConfig,
>;

struct Fixture {
    factory: ProviderFactory<MockNodeTypesWithDB>,
    provider: BlockchainProvider<MockNodeTypesWithDB>,
    filter: ConfiguredContractFilter,
}

impl Fixture {
    fn new() -> Self {
        let factory = create_test_provider_factory_with_chain_spec(DEV.clone());
        let filter = ConfiguredContractFilter::new([TRACKED]);
        let rw = factory.database_provider_rw().unwrap();
        let tx = rw.tx_ref();
        tx.put::<tables::PartialStateAccounts>(
            keccak256(SENDER),
            Account { nonce: 1, balance: U256::from(BALANCE), bytecode_hash: None },
        )
        .unwrap();
        for (address, code) in [
            (TRACKED, delegation(RECIPIENT)),
            (UNTRACKED, delegation(SENDER)),
            (RECIPIENT, Bytes::from_static(&[0x00])),
        ] {
            let hash = keccak256(&code);
            tx.put::<tables::PartialStateAccounts>(
                keccak256(address),
                Account { nonce: 1, balance: U256::from(BALANCE), bytecode_hash: Some(hash) },
            )
            .unwrap();
            // Even physically present code is unavailable unless retained by the filter.
            tx.put::<tables::Bytecodes>(hash, Bytecode::new_raw_checked(code).unwrap()).unwrap();
        }
        let root = rw.partial_state_root(&filter).unwrap();
        let header = SealedHeader::seal_slow(Header {
            state_root: root,
            gas_limit: 30_000_000,
            base_fee_per_gas: Some(1),
            ..Default::default()
        });
        let block = Block { header: header.clone().unseal(), body: Default::default() };
        rw.insert_block(&block.try_into_recovered().unwrap()).unwrap();
        tx.put::<tables::PartialStateCheckpoints>(
            0,
            StoredPartialStateCheckpoint {
                block_number: 0,
                block_hash: header.hash(),
                state_root: root,
                filter_hash: filter.filter_hash(),
                sync_complete: true,
            },
        )
        .unwrap();
        rw.commit().unwrap();
        let provider = BlockchainProvider::with_latest(factory.clone(), header).unwrap();
        Self { factory, provider, filter }
    }

    fn validator(&self, partial: bool) -> Validator {
        EthTransactionValidatorBuilder::new(self.provider.clone(), EthEvmConfig::new(DEV.clone()))
            .with_partial_state_filter(partial.then(|| self.filter.clone()))
            .build(InMemoryBlobStore::default())
    }

    fn checkpoint(&self) -> StoredPartialStateCheckpoint {
        self.factory
            .database_provider_ro()
            .unwrap()
            .tx_ref()
            .get::<tables::PartialStateCheckpoints>(0)
            .unwrap()
            .unwrap()
    }

    fn set_checkpoint(&self, checkpoint: StoredPartialStateCheckpoint) {
        let rw = self.factory.database_provider_rw().unwrap();
        rw.tx_ref().put::<tables::PartialStateCheckpoints>(0, checkpoint).unwrap();
        rw.commit().unwrap();
    }

    fn advance(&self, nonce: u64, complete: bool) {
        let parent = self.checkpoint();
        let rw = self.factory.database_provider_rw().unwrap();
        rw.tx_ref()
            .put::<tables::PartialStateAccounts>(
                keccak256(SENDER),
                Account { nonce, balance: U256::from(BALANCE), bytecode_hash: None },
            )
            .unwrap();
        let root = rw.partial_state_root(&self.filter).unwrap();
        let header = SealedHeader::seal_slow(Header {
            number: parent.block_number + 1,
            parent_hash: parent.block_hash,
            timestamp: parent.block_number + 1,
            state_root: root,
            gas_limit: 30_000_000,
            base_fee_per_gas: Some(1),
            ..Default::default()
        });
        let block = Block { header: header.clone().unseal(), body: Default::default() };
        let executed = ExecutedBlock {
            recovered_block: Arc::new(block.try_into_recovered().unwrap()),
            ..Default::default()
        };
        if complete {
            rw.tx_ref()
                .put::<tables::PartialStateCheckpoints>(
                    0,
                    StoredPartialStateCheckpoint {
                        block_number: header.number,
                        block_hash: header.hash(),
                        state_root: root,
                        ..parent
                    },
                )
                .unwrap();
        }
        rw.commit().unwrap();
        let chain = self.provider.canonical_in_memory_state();
        let new = vec![executed];
        let update = match chain.state_by_number(header.number) {
            Some(old) => NewCanonicalChain::Reorg { new, old: vec![old.block()] },
            None => NewCanonicalChain::Commit { new },
        };
        chain.update_chain(update);
        self.provider.set_canonical_head(header);
    }
}

fn delegation(target: Address) -> Bytes {
    let mut code = vec![0xef, 0x01, 0x00];
    code.extend_from_slice(target.as_slice());
    code.into()
}

fn transaction(sender: Address, nonce: u64) -> EthPooledTransaction {
    let tx = TransactionSigned::new_unhashed(
        Transaction::Legacy(TxLegacy {
            chain_id: Some(DEV.chain().id()),
            nonce,
            gas_limit: 21_000,
            gas_price: 1_000_000_000,
            to: TxKind::Call(RECIPIENT),
            value: U256::from(1),
            ..Default::default()
        }),
        Signature::new(U256::from(1), U256::from(2), false),
    );
    EthPooledTransaction::try_from_consensus(Recovered::new_unchecked(tx, sender)).unwrap()
}

fn assert_unavailable(outcome: TransactionValidationOutcome<EthPooledTransaction>, message: &str) {
    let TransactionValidationOutcome::Error(_, err) = outcome else {
        panic!("expected state availability error, got {outcome:?}");
    };
    assert!(err.downcast_ref::<ProviderError>().is_some(), "{err}");
    assert!(err.to_string().contains(message), "{err}");
}

#[test]
fn partial_state_txpool_accepts_transfer_to_untracked_contract_without_full_state() {
    let f = Fixture::new();
    let ro = f.factory.database_provider_ro().unwrap();
    assert_eq!(ro.tx_ref().entries::<tables::PlainAccountState>().unwrap(), 0);
    assert_eq!(ro.tx_ref().entries::<tables::PlainStorageState>().unwrap(), 0);
    drop(ro);
    let v = f.validator(true);
    for origin in
        [TransactionOrigin::External, TransactionOrigin::Local, TransactionOrigin::Private]
    {
        let outcome = v.validate_one(origin, transaction(SENDER, 1));
        assert!(
            matches!(outcome, TransactionValidationOutcome::Valid {
            state_nonce: 1, balance, ..
        } if balance == U256::from(BALANCE)),
            "{outcome:?}"
        );
    }
}

#[test]
fn partial_state_txpool_checks_nonce_and_balance() {
    let f = Fixture::new();
    let v = f.validator(true);
    let outcome = v.validate_one(TransactionOrigin::External, transaction(SENDER, 0));
    assert!(
        matches!(
            outcome,
            TransactionValidationOutcome::Invalid(
                _,
                InvalidPoolTransactionError::Consensus(
                    InvalidTransactionError::NonceNotConsistent { .. }
                )
            )
        ),
        "{outcome:?}"
    );
    let outcome = v.validate_one(TransactionOrigin::External, transaction(Address::ZERO, 0));
    assert!(
        matches!(
            outcome,
            TransactionValidationOutcome::Invalid(
                _,
                InvalidPoolTransactionError::Consensus(InvalidTransactionError::InsufficientFunds(
                    _
                ))
            )
        ),
        "{outcome:?}"
    );
}

#[test]
fn partial_state_txpool_rejects_incomplete_mismatched_and_missing_checkpoints() {
    let f = Fixture::new();
    let v = f.validator(true);
    let checkpoint = f.checkpoint();
    for (bad, message) in [
        (StoredPartialStateCheckpoint { sync_complete: false, ..checkpoint }, "incomplete"),
        (StoredPartialStateCheckpoint { filter_hash: B256::ZERO, ..checkpoint }, "filter mismatch"),
        (StoredPartialStateCheckpoint { state_root: B256::ZERO, ..checkpoint }, "pivot mismatch"),
        (StoredPartialStateCheckpoint { block_hash: B256::ZERO, ..checkpoint }, "pivot mismatch"),
    ] {
        f.set_checkpoint(bad);
        assert_unavailable(
            v.validate_one(TransactionOrigin::External, transaction(SENDER, 1)),
            message,
        );
    }
    let rw = f.factory.database_provider_rw().unwrap();
    rw.tx_ref().delete::<tables::PartialStateCheckpoints>(0, None).unwrap();
    rw.tx_ref()
        .put::<tables::PlainAccountState>(
            SENDER,
            Account { balance: U256::from(BALANCE), nonce: 1, bytecode_hash: None },
        )
        .unwrap();
    rw.commit().unwrap();
    assert_unavailable(
        v.validate_one(TransactionOrigin::External, transaction(SENDER, 1)),
        "unavailable",
    );
    assert!(f
        .validator(false)
        .validate_one(TransactionOrigin::External, transaction(SENDER, 1))
        .is_valid());
}

#[test]
fn partial_state_txpool_checks_required_sender_delegation_not_recipient_code() {
    let f = Fixture::new();
    let v = f.validator(true);
    assert!(v.validate_one(TransactionOrigin::External, transaction(TRACKED, 1)).is_valid());
    assert_unavailable(
        v.validate_one(TransactionOrigin::External, transaction(UNTRACKED, 1)),
        "not tracked",
    );
    let hash = keccak256(delegation(RECIPIENT));
    let rw = f.factory.database_provider_rw().unwrap();
    rw.tx_ref().delete::<tables::Bytecodes>(hash, None).unwrap();
    rw.commit().unwrap();
    assert_unavailable(
        v.validate_one(TransactionOrigin::External, transaction(TRACKED, 1)),
        "unavailable",
    );
    let rw = f.factory.database_provider_rw().unwrap();
    rw.tx_ref()
        .put::<tables::Bytecodes>(
            hash,
            Bytecode::new_raw_checked(Bytes::from_static(&[0])).unwrap(),
        )
        .unwrap();
    rw.commit().unwrap();
    assert_unavailable(
        v.validate_one(TransactionOrigin::External, transaction(TRACKED, 1)),
        "mismatch",
    );
}

#[test]
fn partial_state_txpool_does_not_reuse_caller_state_after_checkpoint_changes() {
    let f = Fixture::new();
    let v = f.validator(true);
    let parent = f.checkpoint();
    let mut cached =
        Some(Box::new(f.provider.latest().unwrap()) as Box<dyn AccountInfoReader + Send>);
    assert!(v
        .validate_one_with_state(TransactionOrigin::External, transaction(SENDER, 1), &mut cached)
        .is_valid());
    f.advance(2, true);
    assert!(v
        .validate_one_with_state(TransactionOrigin::External, transaction(SENDER, 1), &mut cached)
        .is_invalid());
    assert!(v
        .validate_one_with_state(TransactionOrigin::External, transaction(SENDER, 2), &mut cached)
        .is_valid());
    // Replace the block at the same height, as a canonical reorg would do.
    f.set_checkpoint(parent);
    f.advance(3, true);
    assert!(v
        .validate_one_with_state(TransactionOrigin::External, transaction(SENDER, 2), &mut cached)
        .is_invalid());
    assert!(v
        .validate_one_with_state(TransactionOrigin::External, transaction(SENDER, 3), &mut cached)
        .is_valid());
    f.advance(4, false);
    assert_unavailable(
        v.validate_one_with_state(TransactionOrigin::External, transaction(SENDER, 4), &mut cached),
        "pivot mismatch",
    );
    assert!(cached.is_none());
}

#[test]
fn partial_state_txpool_allows_retained_shared_delegation_code() {
    let f = Fixture::new();
    let rw = f.factory.database_provider_rw().unwrap();
    rw.tx_ref()
        .put::<tables::PartialStateAccounts>(
            keccak256(UNTRACKED),
            Account {
                nonce: 1,
                balance: U256::from(BALANCE),
                bytecode_hash: Some(keccak256(delegation(RECIPIENT))),
            },
        )
        .unwrap();
    rw.commit().unwrap();
    f.advance(2, true);
    assert!(f
        .validator(true)
        .validate_one(TransactionOrigin::External, transaction(UNTRACKED, 1))
        .is_valid());
}

#[test]
fn partial_state_txpool_rejects_readable_non_delegation_sender_code() {
    let mut f = Fixture::new();
    f.filter = ConfiguredContractFilter::new([RECIPIENT]);
    f.set_checkpoint(StoredPartialStateCheckpoint {
        filter_hash: f.filter.filter_hash(),
        ..f.checkpoint()
    });
    let outcome =
        f.validator(true).validate_one(TransactionOrigin::External, transaction(RECIPIENT, 1));
    assert!(
        matches!(
            outcome,
            TransactionValidationOutcome::Invalid(
                _,
                InvalidPoolTransactionError::Consensus(
                    InvalidTransactionError::SignerAccountHasBytecode
                )
            )
        ),
        "{outcome:?}"
    );
}

#[test]
fn partial_state_txpool_explicit_providers_cannot_bypass_checkpoint_validation() {
    let f = Fixture::new();
    let v = f.validator(true);
    let full = f.provider.latest().unwrap();
    assert!(v
        .validate_one_with_state_provider(
            TransactionOrigin::External,
            transaction(SENDER, 1),
            &full
        )
        .is_valid());
    assert!(v
        .validate_stateful(TransactionOrigin::External, transaction(SENDER, 1), &full)
        .is_valid());
    f.set_checkpoint(StoredPartialStateCheckpoint { sync_complete: false, ..f.checkpoint() });
    assert_unavailable(
        v.validate_one_with_state_provider(
            TransactionOrigin::External,
            transaction(SENDER, 1),
            &full,
        ),
        "incomplete",
    );
    assert_unavailable(
        v.validate_stateful(TransactionOrigin::External, transaction(SENDER, 1), &full),
        "incomplete",
    );
}

#[test]
fn partial_state_txpool_batch_holds_one_checkpoint_and_next_batch_refreshes() {
    let f = Fixture::new();
    let mut v = f.validator(true);
    let changed = Arc::new(AtomicBool::new(false));
    let change_once = changed.clone();
    v.set_additional_stateful_validation(move |_, _, _| {
        if !change_once.swap(true, std::sync::atomic::Ordering::SeqCst) {
            f.advance(2, true);
        }
        Ok(())
    });
    let results = v.validate_batch_with_origin(
        TransactionOrigin::External,
        [transaction(SENDER, 1), transaction(SENDER, 1)],
    );
    assert!(results.iter().all(TransactionValidationOutcome::is_valid), "{results:?}");
    let results = v.validate_batch([(TransactionOrigin::External, transaction(SENDER, 1))]);
    assert!(results[0].is_invalid(), "{results:?}");
}

#[tokio::test]
async fn partial_state_txpool_unavailable_state_is_not_a_bad_transaction() {
    let f = Fixture::new();
    f.set_checkpoint(StoredPartialStateCheckpoint { sync_complete: false, ..f.checkpoint() });
    let pool = Pool::new(
        f.validator(true),
        CoinbaseTipOrdering::default(),
        InMemoryBlobStore::default(),
        Default::default(),
    );
    let tx = transaction(SENDER, 1);
    let err = pool.add_external_transaction(tx.clone()).await.unwrap_err();
    assert!(matches!(err.kind, PoolErrorKind::Other(_)), "{err:?}");
    assert!(!err.is_bad_transaction());
    assert!(pool.get(tx.hash()).is_none());
    f.set_checkpoint(StoredPartialStateCheckpoint { sync_complete: true, ..f.checkpoint() });
    pool.add_external_transaction(tx).await.unwrap();
}
