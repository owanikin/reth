use crate::{
    partial_state::{ensure_complete_checkpoint, read_partial_state_checkpoint},
    providers::ProviderNodeTypes,
    DatabaseProvider, ProviderFactory,
};
use alloy_consensus::constants::EMPTY_ROOT_HASH;
use alloy_primitives::{keccak256, Address};
use reth_db_api::{
    cursor::{DbCursorRO, DbDupCursorRO},
    models::BlockNumberAddress,
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_storage_api::{
    ContractFilter, DBProvider, DatabaseProviderFactory, PartialStateRootProvider,
    PartialStateSnapPivot,
};
use reth_storage_errors::provider::{
    PartialStateCheckpointError, PartialStateReadError, ProviderError, ProviderResult,
};
use std::collections::BTreeSet;

/// Logical rows removed by an explicit partial-storage expiry operation.
///
/// These counts do not measure reclaimed filesystem space. Existing readers can retain old
/// database pages until their snapshots are released.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PartialStateStorageExpiryOutcome {
    /// Non-empty accounts changed from available storage to unavailable storage.
    pub accounts_expired: usize,
    /// Current storage slots deleted, including any stale rows for unavailable accounts.
    pub slots_deleted: usize,
    /// Historical slot before-images deleted from retained rollback journals.
    pub journal_slots_deleted: usize,
}

impl<N: ProviderNodeTypes> ProviderFactory<N> {
    /// Validates and counts an explicit storage expiry without opening a write transaction.
    ///
    /// This is a snapshot preview, not a reservation. Applying expiry must revalidate the pivot
    /// and filter, and may report different counts if another retention operation ran meanwhile.
    pub fn preview_partial_state_storage_expiry(
        &self,
        expected: PartialStateSnapPivot,
        filter: &dyn ContractFilter,
        addresses: &[Address],
    ) -> ProviderResult<PartialStateStorageExpiryOutcome> {
        inspect_expiry(
            &self.database_provider_ro()?,
            expected,
            filter,
            &addresses.iter().copied().collect(),
        )
    }

    /// Expires storage for explicitly selected tracked addresses at a verified checkpoint.
    ///
    /// Deletes current slots and their retained rollback before-images, and updates current and
    /// journaled availability in one transaction. Account/storage-root commitments, bytecode,
    /// headers, and checkpoint identity are unchanged. Reverting a retained transition still
    /// restores its account commitments, but cannot resurrect the expired storage.
    ///
    /// The caller must supply the current pivot and filter: a concurrent advancement or reorg
    /// makes a stale request fail without modifying state. The root is verified before and after
    /// deletion. Duplicate addresses are processed once; absent and known-empty accounts remain
    /// readable as empty, while any retained non-empty history for them is expired as well.
    /// Repeating the operation is idempotent. Untracked addresses are rejected.
    ///
    /// This is a blocking, explicit storage operation, not a timer or retention policy. It scans
    /// retained account journals and computes the complete partial root twice; async callers
    /// must dispatch it to a blocking task. Existing snapshot readers keep their pre-expiry view.
    pub fn expire_partial_state_storage(
        &self,
        expected: PartialStateSnapPivot,
        filter: &dyn ContractFilter,
        addresses: &[Address],
    ) -> ProviderResult<PartialStateStorageExpiryOutcome> {
        let addresses: BTreeSet<_> = addresses.iter().copied().collect();
        let provider = self.database_provider_rw()?;
        let outcome = inspect_expiry(&provider, expected, filter, &addresses)?;

        for address in &addresses {
            let hash = keccak256(address);
            if provider.tx_ref().get::<tables::PartialStateAccounts>(hash)?.is_some() {
                let mut availability = provider
                    .tx_ref()
                    .get::<tables::PartialStateAvailability>(hash)?
                    .ok_or(PartialStateReadError::MissingAvailability(hash))?;
                let root = provider
                    .tx_ref()
                    .get::<tables::PartialStateStorageRoots>(hash)?
                    .unwrap_or(EMPTY_ROOT_HASH);
                availability.storage = root == EMPTY_ROOT_HASH;
                provider.tx_ref().put::<tables::PartialStateAvailability>(hash, availability)?;
            }
            provider.tx_ref().delete::<tables::PartialStateStorages>(hash, None)?;
        }

        // Expiry is a local retention decision, not a canonical transition to undo. Redact old
        // before-images too, otherwise a reorg could restore availability over incomplete slots.
        let mut journal =
            provider.tx_ref().cursor_read::<tables::PartialStateAccountChangeSets>()?;
        for entry in journal.walk(None)? {
            let (number, before) = entry?;
            if !addresses.contains(&before.address) {
                continue
            }
            let key = BlockNumberAddress((number, before.address));
            let mut availability = provider
                .tx_ref()
                .get::<tables::PartialStateAvailabilityChangeSets>(key)?
                .ok_or(PartialStateReadError::MissingAvailability(keccak256(before.address)))?;
            availability.storage = before.storage_root.is_none();
            provider
                .tx_ref()
                .put::<tables::PartialStateAvailabilityChangeSets>(key, availability)?;
            provider.tx_ref().delete::<tables::PartialStateStorageChangeSets>(key, None)?;
        }
        drop(journal);

        let computed = provider.partial_state_root(filter)?;
        if computed != expected.state_root {
            return Err(PartialStateCheckpointError::RootMismatch {
                expected: expected.state_root,
                computed,
            }
            .into())
        }
        provider.commit()?;
        Ok(outcome)
    }
}

fn inspect_expiry<TX: DbTx + Send + Sync + 'static, N: ProviderNodeTypes>(
    provider: &DatabaseProvider<TX, N>,
    expected: PartialStateSnapPivot,
    filter: &dyn ContractFilter,
    addresses: &BTreeSet<Address>,
) -> ProviderResult<PartialStateStorageExpiryOutcome> {
    for address in addresses {
        if !filter.should_sync_storage(address) {
            return Err(ProviderError::StorageNotTracked(*address))
        }
    }
    let checkpoint =
        read_partial_state_checkpoint(provider)?.ok_or(PartialStateCheckpointError::Unavailable)?;
    ensure_complete_checkpoint(checkpoint, expected, filter)?;
    let computed = provider.partial_state_root(filter)?;
    if computed != expected.state_root {
        return Err(PartialStateCheckpointError::RootMismatch {
            expected: expected.state_root,
            computed,
        }
        .into())
    }

    let mut outcome = PartialStateStorageExpiryOutcome::default();
    for address in addresses {
        let hash = keccak256(address);
        if provider.tx_ref().get::<tables::PartialStateAccounts>(hash)?.is_some() {
            let availability = provider
                .tx_ref()
                .get::<tables::PartialStateAvailability>(hash)?
                .ok_or(PartialStateReadError::MissingAvailability(hash))?;
            let root = provider
                .tx_ref()
                .get::<tables::PartialStateStorageRoots>(hash)?
                .unwrap_or(EMPTY_ROOT_HASH);
            if availability.storage && root != EMPTY_ROOT_HASH {
                outcome.accounts_expired += 1;
            }
        }
        outcome.slots_deleted += provider
            .tx_ref()
            .cursor_dup_read::<tables::PartialStateStorages>()?
            .walk_dup(Some(hash), None)?
            .try_fold(0, |count, entry| entry.map(|_| count + 1))?;
    }
    let mut journal = provider.tx_ref().cursor_read::<tables::PartialStateAccountChangeSets>()?;
    for entry in journal.walk(None)? {
        let (number, before) = entry?;
        if addresses.contains(&before.address) {
            let key = BlockNumberAddress((number, before.address));
            provider
                .tx_ref()
                .get::<tables::PartialStateAvailabilityChangeSets>(key)?
                .ok_or(PartialStateReadError::MissingAvailability(keccak256(before.address)))?;
            outcome.journal_slots_deleted += provider
                .tx_ref()
                .cursor_dup_read::<tables::PartialStateStorageChangeSets>()?
                .walk_dup(Some(key), None)?
                .try_fold(0, |count, entry| entry.map(|_| count + 1))?;
        }
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests;
