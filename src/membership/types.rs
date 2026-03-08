use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Instant;

/// Stable identifier of a cluster node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

impl NodeId {
    /// Creates a new random node identifier.
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

/// SWIM-like liveness state of a node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeState {
    /// Node is considered healthy and reachable.
    Alive,
    /// Node is suspected to be unavailable.
    Suspect,
    /// Node is considered unavailable.
    Dead,
}

/// Membership entry describing one node in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// Logical node identifier.
    pub id: NodeId,
    /// UDP socket used for gossip traffic.
    pub gossip_addr: SocketAddr,
    /// HTTP socket used for replication and task APIs.
    pub http_addr: SocketAddr,
    /// Current liveness state.
    pub state: NodeState,
    /// Monotonic incarnation counter for conflict resolution.
    pub incarnation: u64,

    /// Local timestamp of the last successful contact.
    ///
    /// This field is skipped during serialization.
    #[serde(skip)]
    pub last_seen: Option<Instant>,
}

/// Messages exchanged between nodes over UDP gossip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    /// Health probe sent to another node.
    Ping {
        /// Sender node id.
        from: NodeId,
        /// Sender incarnation.
        incarnation: u64,
    },

    /// Reply to `Ping` carrying local membership view.
    Ack {
        /// Sender node id.
        from: NodeId,
        /// Sender incarnation.
        incarnation: u64,
        /// Sender membership snapshot.
        members: Vec<Node>,
    },

    /// Join request used by a new node.
    Join {
        /// Joining node descriptor.
        node: Node,
    },

    /// Suspect broadcast for a node.
    Suspect {
        /// Suspected node id.
        node_id: NodeId,
        /// Incarnation used to compare freshness.
        incarnation: u64,
    },

    /// Alive broadcast used to refute suspicion.
    Alive {
        /// Node id that is alive.
        node_id: NodeId,
        /// Incarnation used to compare freshness.
        incarnation: u64,
    },
}
