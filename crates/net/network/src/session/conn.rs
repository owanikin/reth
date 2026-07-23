//! Connection types for a session

use futures::{Sink, Stream};
use reth_ecies::stream::ECIESStream;
use reth_eth_wire::{
    errors::EthStreamError,
    eth_snap_stream::{EthSnapMessage, EthSnapStream},
    message::EthBroadcastMessage,
    multiplex::{ProtocolProxy, RlpxSatelliteStream},
    EthMessage, EthNetworkPrimitives, EthStream, EthVersion, NetworkPrimitives, P2PStream,
};
use reth_eth_wire_types::RawCapabilityMessage;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::net::TcpStream;

/// The type of the underlying peer network connection.
pub type EthPeerConnection<N> = EthStream<P2PStream<ECIESStream<TcpStream>>, N>;

/// A connection that supports the ETH protocol and the dependent SNAP protocol.
pub type EthSnapPeerConnection<N> = EthSnapStream<P2PStream<ECIESStream<TcpStream>>, N>;

/// Various connection types that at least support the ETH protocol.
pub type EthSatelliteConnection<N = EthNetworkPrimitives> =
    RlpxSatelliteStream<ECIESStream<TcpStream>, EthStream<ProtocolProxy, N>>;

/// Connection types that support the ETH protocol.
///
/// This can be either:
/// - A connection that only supports the ETH protocol
/// - A connection that supports the ETH protocol and at least one other `RLPx` protocol
// This type is boxed because the underlying stream is ~6KB,
// mostly coming from `P2PStream`'s `snap::Encoder` (2072), and `ECIESStream` (3600).
#[derive(Debug)]
pub enum EthRlpxConnection<N: NetworkPrimitives = EthNetworkPrimitives> {
    /// A connection that only supports the ETH protocol.
    EthOnly(Box<EthPeerConnection<N>>),
    /// A connection that supports ETH plus the dependent SNAP protocol.
    EthSnap(Box<EthSnapPeerConnection<N>>),
    /// A connection that supports the ETH protocol and __at least one other__ `RLPx` protocol.
    Satellite(Box<EthSatelliteConnection<N>>),
}

impl<N: NetworkPrimitives> EthRlpxConnection<N> {
    /// Returns the negotiated ETH version.
    #[inline]
    pub(crate) const fn version(&self) -> EthVersion {
        match self {
            Self::EthOnly(conn) => conn.version(),
            Self::EthSnap(conn) => conn.eth_version(),
            Self::Satellite(conn) => conn.primary().version(),
        }
    }

    /// Consumes this type and returns the wrapped [`P2PStream`].
    #[inline]
    pub(crate) fn into_inner(self) -> P2PStream<ECIESStream<TcpStream>> {
        match self {
            Self::EthOnly(conn) => conn.into_inner(),
            Self::EthSnap(conn) => conn.into_inner(),
            Self::Satellite(conn) => conn.into_inner(),
        }
    }

    /// Returns mutable access to the underlying stream.
    #[inline]
    pub(crate) fn inner_mut(&mut self) -> &mut P2PStream<ECIESStream<TcpStream>> {
        match self {
            Self::EthOnly(conn) => conn.inner_mut(),
            Self::EthSnap(conn) => conn.inner_mut(),
            Self::Satellite(conn) => conn.inner_mut(),
        }
    }

    /// Returns access to the underlying stream.
    #[inline]
    pub(crate) const fn inner(&self) -> &P2PStream<ECIESStream<TcpStream>> {
        match self {
            Self::EthOnly(conn) => conn.inner(),
            Self::EthSnap(conn) => conn.inner(),
            Self::Satellite(conn) => conn.inner(),
        }
    }

    /// Same as [`Sink::start_send`] but accepts a [`EthBroadcastMessage`] instead.
    #[inline]
    pub fn start_send_broadcast(
        &mut self,
        item: EthBroadcastMessage<N>,
    ) -> Result<(), EthStreamError> {
        match self {
            Self::EthOnly(conn) => conn.start_send_broadcast(item),
            Self::EthSnap(conn) => conn.start_send_broadcast(item).map_err(Into::into),
            Self::Satellite(conn) => conn.primary_mut().start_send_broadcast(item),
        }
    }

    /// Sends a raw capability message over the connection
    pub fn start_send_raw(&mut self, msg: RawCapabilityMessage) -> Result<(), EthStreamError> {
        match self {
            Self::EthOnly(conn) => conn.start_send_raw(msg),
            Self::EthSnap(conn) => conn.start_send_raw(msg).map_err(Into::into),
            Self::Satellite(conn) => conn.primary_mut().start_send_raw(msg),
        }
    }

    /// Sets whether to reject block announcement messages (`NewBlock`, `NewBlockHashes`) before
    /// RLP decoding to avoid memory amplification from deserializing blocks that will be discarded.
    pub fn set_reject_block_announcements(&mut self, reject: bool) {
        match self {
            Self::EthOnly(conn) => conn.set_reject_block_announcements(reject),
            Self::EthSnap(conn) => conn.set_reject_block_announcements(reject),
            Self::Satellite(conn) => conn.primary_mut().set_reject_block_announcements(reject),
        }
    }
}

impl<N: NetworkPrimitives> From<EthPeerConnection<N>> for EthRlpxConnection<N> {
    #[inline]
    fn from(conn: EthPeerConnection<N>) -> Self {
        Self::EthOnly(Box::new(conn))
    }
}

impl<N: NetworkPrimitives> From<EthSnapPeerConnection<N>> for EthRlpxConnection<N> {
    #[inline]
    fn from(conn: EthSnapPeerConnection<N>) -> Self {
        Self::EthSnap(Box::new(conn))
    }
}

impl<N: NetworkPrimitives> From<EthSatelliteConnection<N>> for EthRlpxConnection<N> {
    #[inline]
    fn from(conn: EthSatelliteConnection<N>) -> Self {
        Self::Satellite(Box::new(conn))
    }
}

impl<N: NetworkPrimitives> Stream for EthRlpxConnection<N> {
    type Item = Result<EthSnapMessage<N>, EthStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        unsafe {
            match self.get_unchecked_mut() {
                Self::EthOnly(conn) => Pin::new_unchecked(conn)
                    .poll_next(cx)
                    .map(|res| res.map(|msg| msg.map(EthSnapMessage::Eth))),
                Self::EthSnap(conn) => Pin::new_unchecked(conn)
                    .poll_next(cx)
                    .map(|res| res.map(|msg| msg.map_err(Into::into))),
                Self::Satellite(conn) => Pin::new_unchecked(conn)
                    .poll_next(cx)
                    .map(|res| res.map(|msg| msg.map(EthSnapMessage::Eth))),
            }
        }
    }
}

impl<N: NetworkPrimitives> Sink<EthMessage<N>> for EthRlpxConnection<N> {
    type Error = EthStreamError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        unsafe {
            match self.get_unchecked_mut() {
                Self::EthOnly(conn) => Pin::new_unchecked(conn).poll_ready(cx),
                Self::EthSnap(conn) => Pin::new_unchecked(conn).poll_ready(cx).map_err(Into::into),
                Self::Satellite(conn) => Pin::new_unchecked(conn).poll_ready(cx),
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: EthMessage<N>) -> Result<(), Self::Error> {
        unsafe {
            match self.get_unchecked_mut() {
                Self::EthOnly(conn) => Pin::new_unchecked(conn).start_send(item),
                Self::EthSnap(conn) => Pin::new_unchecked(conn)
                    .start_send(EthSnapMessage::Eth(item))
                    .map_err(Into::into),
                Self::Satellite(conn) => Pin::new_unchecked(conn).start_send(item),
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        unsafe {
            match self.get_unchecked_mut() {
                Self::EthOnly(conn) => Pin::new_unchecked(conn).poll_flush(cx),
                Self::EthSnap(conn) => Pin::new_unchecked(conn).poll_flush(cx).map_err(Into::into),
                Self::Satellite(conn) => Pin::new_unchecked(conn).poll_flush(cx),
            }
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        unsafe {
            match self.get_unchecked_mut() {
                Self::EthOnly(conn) => Pin::new_unchecked(conn).poll_close(cx),
                Self::EthSnap(conn) => Pin::new_unchecked(conn).poll_close(cx).map_err(Into::into),
                Self::Satellite(conn) => Pin::new_unchecked(conn).poll_close(cx),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn assert_eth_stream<N, St>()
    where
        N: NetworkPrimitives,
        St: Stream<Item = Result<EthMessage<N>, EthStreamError>> + Sink<EthMessage<N>>,
    {
    }

    const fn assert_eth_snap_stream<N, St>()
    where
        N: NetworkPrimitives,
        St: Stream<Item = Result<EthSnapMessage<N>, EthStreamError>> + Sink<EthMessage<N>>,
    {
    }

    #[test]
    const fn test_eth_stream_variants() {
        assert_eth_stream::<EthNetworkPrimitives, EthSatelliteConnection<EthNetworkPrimitives>>();
        assert_eth_snap_stream::<EthNetworkPrimitives, EthRlpxConnection<EthNetworkPrimitives>>();
    }
}
