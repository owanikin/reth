//! Non-validating Engine API ingress. No executor, pipeline or payload builder is reachable here.

use super::partial_state::retain_payload_bal;
use alloy_primitives::B256;
use alloy_rpc_types_engine::{PayloadStatus, PayloadStatusEnum};
use reth_engine_primitives::{
    BeaconEngineMessage, ExecutionPayload, NewPayloadTimings, OnForkChoiceUpdated,
};
use reth_node_api::PayloadTypes;
use reth_storage_api::BalStoreHandle;
use std::time::Duration;
use tokio::sync::watch;

/// Accepts an operator-trusted target without asserting execution validity. Even after root
/// verification, responses remain SYNCING: commitments are not an execution validity proof.
pub(super) fn on_message<P: PayloadTypes>(
    message: BeaconEngineMessage<P>,
    targets: &watch::Sender<Option<B256>>,
    bals: &BalStoreHandle,
) {
    let syncing = || PayloadStatus::from_status(PayloadStatusEnum::Syncing);
    match message {
        BeaconEngineMessage::ForkchoiceUpdated { state, tx, .. } => {
            if state.head_block_hash.is_zero() {
                let _ = tx.send(Ok(OnForkChoiceUpdated::invalid_state()));
                return
            }
            // Attributes are deliberately not forwarded to a payload builder.
            targets.send_if_modified(|target| {
                if *target == Some(state.head_block_hash) {
                    return false
                }
                *target = Some(state.head_block_hash);
                true
            });
            let _ = tx.send(Ok(OnForkChoiceUpdated::syncing()));
        }
        BeaconEngineMessage::NewPayload { payload, tx } => {
            if let Some(raw) = payload.block_access_list() {
                retain_payload_bal(bals, payload.num_hash(), raw);
            }
            let _ = tx.send(Ok(syncing()));
        }
        BeaconEngineMessage::RethNewPayload { payload, tx, .. } => {
            if let Some(raw) = payload.block_access_list() {
                retain_payload_bal(bals, payload.num_hash(), raw);
            }
            let _ = tx.send(Ok((
                syncing(),
                NewPayloadTimings {
                    latency: Duration::ZERO,
                    persistence_wait: Duration::ZERO,
                    execution_cache_wait: None,
                    sparse_trie_wait: None,
                },
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_rpc_types_engine::{ExecutionData, ForkchoiceState, PayloadAttributes};
    use reth_ethereum_engine_primitives::EthEngineTypes;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn partial_state_follower_forkchoice_never_validates_or_builds() {
        let (targets, mut rx) = watch::channel(None);
        let bals = BalStoreHandle::default();
        let hash = B256::repeat_byte(1);
        for attributes in [None, Some(PayloadAttributes::default())] {
            let (tx, response) = oneshot::channel();
            on_message::<EthEngineTypes>(
                BeaconEngineMessage::ForkchoiceUpdated {
                    state: ForkchoiceState { head_block_hash: hash, ..Default::default() },
                    payload_attrs: attributes,
                    tx,
                },
                &targets,
                &bals,
            );
            let result = response.await.unwrap().unwrap().await.unwrap();
            assert_eq!(result.payload_status.status, PayloadStatusEnum::Syncing);
            assert!(result.payload_status.latest_valid_hash.is_none());
            assert!(result.payload_id.is_none());
            assert_eq!(*rx.borrow_and_update(), Some(hash));
        }
        // A repeated FCU must not interrupt outstanding peer retries.
        let (tx, response) = oneshot::channel();
        on_message::<EthEngineTypes>(
            BeaconEngineMessage::ForkchoiceUpdated {
                state: ForkchoiceState { head_block_hash: hash, ..Default::default() },
                payload_attrs: None,
                tx,
            },
            &targets,
            &bals,
        );
        assert!(!rx.has_changed().unwrap());
        let _ = response.await.unwrap().unwrap().await.unwrap();

        let (tx, response) = oneshot::channel();
        on_message::<EthEngineTypes>(
            BeaconEngineMessage::ForkchoiceUpdated {
                state: ForkchoiceState::default(),
                payload_attrs: None,
                tx,
            },
            &targets,
            &bals,
        );
        assert!(response.await.unwrap().unwrap().await.is_err());
        assert_eq!(*rx.borrow(), Some(hash));
    }

    #[tokio::test]
    async fn partial_state_follower_payloads_do_not_select_a_head_or_claim_execution() {
        let (targets, rx) = watch::channel(None);
        let bals = BalStoreHandle::default();
        let payload = ExecutionData::from_block_unchecked(
            B256::repeat_byte(1),
            &reth_ethereum_primitives::Block::default(),
        );
        let (tx, response) = oneshot::channel();
        on_message::<EthEngineTypes>(
            BeaconEngineMessage::NewPayload { payload: payload.clone(), tx },
            &targets,
            &bals,
        );
        let status = response.await.unwrap().unwrap();
        assert_eq!(status.status, PayloadStatusEnum::Syncing);
        assert!(status.latest_valid_hash.is_none());
        assert_eq!(*rx.borrow(), None);
        let (tx, response) = oneshot::channel();
        on_message::<EthEngineTypes>(
            BeaconEngineMessage::RethNewPayload {
                payload,
                tx,
                wait_for_persistence: true,
                wait_for_caches: true,
                enqueued_at: std::time::Instant::now(),
            },
            &targets,
            &bals,
        );
        let (status, timing) = response.await.unwrap().unwrap();
        assert_eq!(status.status, PayloadStatusEnum::Syncing);
        assert!(status.latest_valid_hash.is_none());
        assert_eq!(timing.persistence_wait, Duration::ZERO);
        assert!(timing.execution_cache_wait.is_none());
        assert_eq!(*rx.borrow(), None);
    }
}
