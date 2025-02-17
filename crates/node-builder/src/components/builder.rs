//! A generic [NodeComponentsBuilder]

use crate::{
    components::{NetworkBuilder, NodeComponents, PayloadServiceBuilder, PoolBuilder},
    BuilderContext, FullNodeTypes,
};
use reth_transaction_pool::TransactionPool;
use std::marker::PhantomData;

/// A generic, customizable [`NodeComponentsBuilder`].
///
/// This type is stateful and captures the configuration of the node's components.
///
/// ## Component dependencies:
///
/// The components of the node depend on each other:
/// - The payload builder service depends on the transaction pool.
/// - The network depends on the transaction pool.
///
/// We distinguish between different kind of components:
/// - Components that are standalone, such as the transaction pool.
/// - Components that are spawned as a service, such as the payload builder service or the network.
///
/// ## Builder lifecycle:
///
/// First all standalone components are built. Then the service components are spawned.
/// All component builders are captured in the builder state and will be consumed once the node is
/// launched.
#[derive(Debug)]
pub struct ComponentsBuilder<Node, PoolB, PayloadB, NetworkB> {
    pool_builder: PoolB,
    payload_builder: PayloadB,
    network_builder: NetworkB,
    _marker: PhantomData<Node>,
}

impl<Node, PoolB, PayloadB, NetworkB> ComponentsBuilder<Node, PoolB, PayloadB, NetworkB> {
    /// Configures the node types.
    pub fn node_types<Types>(self) -> ComponentsBuilder<Types, PoolB, PayloadB, NetworkB>
    where
        Types: FullNodeTypes,
    {
        let Self { pool_builder, payload_builder, network_builder, _marker } = self;
        ComponentsBuilder {
            pool_builder,
            payload_builder,
            network_builder,
            _marker: Default::default(),
        }
    }

    /// Apply a function to the pool builder.
    pub fn map_pool(self, f: impl FnOnce(PoolB) -> PoolB) -> Self {
        Self {
            pool_builder: f(self.pool_builder),
            payload_builder: self.payload_builder,
            network_builder: self.network_builder,
            _marker: self._marker,
        }
    }

    /// Apply a function to the payload builder.
    pub fn map_payload(self, f: impl FnOnce(PayloadB) -> PayloadB) -> Self {
        Self {
            pool_builder: self.pool_builder,
            payload_builder: f(self.payload_builder),
            network_builder: self.network_builder,
            _marker: self._marker,
        }
    }

    /// Apply a function to the network builder.
    pub fn map_network(self, f: impl FnOnce(NetworkB) -> NetworkB) -> Self {
        Self {
            pool_builder: self.pool_builder,
            payload_builder: self.payload_builder,
            network_builder: f(self.network_builder),
            _marker: self._marker,
        }
    }
}

impl<Node, PoolB, PayloadB, NetworkB> ComponentsBuilder<Node, PoolB, PayloadB, NetworkB>
where
    Node: FullNodeTypes,
{
    /// Configures the pool builder.
    ///
    /// This accepts a [PoolBuilder] instance that will be used to create the node's transaction
    /// pool.
    pub fn pool<PB>(self, pool_builder: PB) -> ComponentsBuilder<Node, PB, PayloadB, NetworkB>
    where
        PB: PoolBuilder<Node>,
    {
        let Self { pool_builder: _, payload_builder, network_builder, _marker } = self;
        ComponentsBuilder { pool_builder, payload_builder, network_builder, _marker }
    }
}

impl<Node, PoolB, PayloadB, NetworkB> ComponentsBuilder<Node, PoolB, PayloadB, NetworkB>
where
    Node: FullNodeTypes,
    PoolB: PoolBuilder<Node>,
{
    /// Configures the network builder.
    ///
    /// This accepts a [NetworkBuilder] instance that will be used to create the node's network
    /// stack.
    pub fn network<NB>(self, network_builder: NB) -> ComponentsBuilder<Node, PoolB, PayloadB, NB>
    where
        NB: NetworkBuilder<Node, PoolB::Pool>,
    {
        let Self { pool_builder, payload_builder, network_builder: _, _marker } = self;
        ComponentsBuilder { pool_builder, payload_builder, network_builder, _marker }
    }

    /// Configures the payload builder.
    ///
    /// This accepts a [PayloadServiceBuilder] instance that will be used to create the node's
    /// payload builder service.
    pub fn payload<PB>(self, payload_builder: PB) -> ComponentsBuilder<Node, PoolB, PB, NetworkB>
    where
        PB: PayloadServiceBuilder<Node, PoolB::Pool>,
    {
        let Self { pool_builder, payload_builder: _, network_builder, _marker } = self;
        ComponentsBuilder { pool_builder, payload_builder, network_builder, _marker }
    }
}

impl<Node, PoolB, PayloadB, NetworkB> NodeComponentsBuilder<Node>
    for ComponentsBuilder<Node, PoolB, PayloadB, NetworkB>
where
    Node: FullNodeTypes,
    PoolB: PoolBuilder<Node>,
    NetworkB: NetworkBuilder<Node, PoolB::Pool>,
    PayloadB: PayloadServiceBuilder<Node, PoolB::Pool>,
{
    type Pool = PoolB::Pool;

    async fn build_components(
        self,
        context: &BuilderContext<Node>,
    ) -> eyre::Result<NodeComponents<Node, Self::Pool>> {
        let Self { pool_builder, payload_builder, network_builder, _marker } = self;

        let pool = pool_builder.build_pool(context).await?;
        let network = network_builder.build_network(context, pool.clone()).await?;
        let payload_builder = payload_builder.spawn_payload_service(context, pool.clone()).await?;

        Ok(NodeComponents { transaction_pool: pool, network, payload_builder })
    }
}

impl Default for ComponentsBuilder<(), (), (), ()> {
    fn default() -> Self {
        Self {
            pool_builder: (),
            payload_builder: (),
            network_builder: (),
            _marker: Default::default(),
        }
    }
}

/// A type that configures all the customizable components of the node and knows how to build them.
///
/// Implementers of this trait are responsible for building all the components of the node: See
/// [NodeComponents].
///
/// The [ComponentsBuilder] is a generic implementation of this trait that can be used to customize
/// certain components of the node using the builder pattern and defaults, e.g. Ethereum and
/// Optimism.
pub trait NodeComponentsBuilder<Node: FullNodeTypes> {
    /// The transaction pool to use.
    type Pool: TransactionPool + Unpin + 'static;

    /// Builds the components of the node.
    fn build_components(
        self,
        context: &BuilderContext<Node>,
    ) -> impl std::future::Future<Output = eyre::Result<NodeComponents<Node, Self::Pool>>> + Send;
}

impl<Node, F, Fut, Pool> NodeComponentsBuilder<Node> for F
where
    Node: FullNodeTypes,
    F: FnOnce(&BuilderContext<Node>) -> Fut + Send,
    Fut: std::future::Future<Output = eyre::Result<NodeComponents<Node, Pool>>> + Send,
    Pool: TransactionPool + Unpin + 'static,
{
    type Pool = Pool;

    fn build_components(
        self,
        ctx: &BuilderContext<Node>,
    ) -> impl std::future::Future<Output = eyre::Result<NodeComponents<Node, Self::Pool>>> + Send
    {
        self(ctx)
    }
}
