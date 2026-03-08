#[cfg(test)]
mod tests {
    use crate::membership::service::MembershipService;
    use crate::membership::types::{GossipMessage, Node, NodeId, NodeState};
    use std::net::SocketAddr;
    use std::time::Instant;

    // ============================================================
    // NODE ID TESTS
    // ============================================================

    #[test]
    fn test_node_id_is_unique() {
        let id1 = NodeId::new();
        let id2 = NodeId::new();

        assert_ne!(id1, id2, "Każde NodeId powinno być unikalne");
    }

    #[test]
    fn test_node_id_equality() {
        let id1 = NodeId("test-123".to_string());
        let id2 = NodeId("test-123".to_string());
        let id3 = NodeId("test-456".to_string());

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_node_id_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        let id1 = NodeId("node-1".to_string());
        let id2 = NodeId("node-1".to_string()); // same value
        let id3 = NodeId("node-2".to_string());

        set.insert(id1.clone());
        set.insert(id2); // should not increase size (duplicate)
        set.insert(id3);

        assert_eq!(set.len(), 2, "HashSet powinien mieć 2 unikalne NodeId");
    }

    // ============================================================
    // NODE STATE TESTS
    // ============================================================

    #[test]
    fn test_node_state_equality() {
        assert_eq!(NodeState::Alive, NodeState::Alive);
        assert_eq!(NodeState::Suspect, NodeState::Suspect);
        assert_eq!(NodeState::Dead, NodeState::Dead);

        assert_ne!(NodeState::Alive, NodeState::Suspect);
        assert_ne!(NodeState::Alive, NodeState::Dead);
        assert_ne!(NodeState::Suspect, NodeState::Dead);
    }

    // ============================================================
    // NODE TESTS
    // ============================================================

    #[test]
    fn test_node_creation() {
        let node = Node {
            id: NodeId::new(),
            gossip_addr: "127.0.0.1:5000".parse().unwrap(),
            http_addr: "127.0.0.1:6000".parse().unwrap(),
            state: NodeState::Alive,
            incarnation: 1,
            last_seen: Some(Instant::now()),
        };

        assert_eq!(node.state, NodeState::Alive);
        assert_eq!(node.incarnation, 1);
    }

    #[test]
    fn test_node_serialization() {
        let node = Node {
            id: NodeId("test-node".to_string()),
            gossip_addr: "192.168.1.1:5000".parse().unwrap(),
            http_addr: "192.168.1.1:6000".parse().unwrap(),
            state: NodeState::Alive,
            incarnation: 42,
            last_seen: Some(Instant::now()),
        };

        // Serialize
        let json = serde_json::to_string(&node).expect("Serialization failed");

        // Deserialize
        let restored: Node = serde_json::from_str(&json).expect("Deserialization failed");

        assert_eq!(restored.id, node.id);
        assert_eq!(restored.gossip_addr, node.gossip_addr);
        assert_eq!(restored.http_addr, node.http_addr);
        assert_eq!(restored.state, node.state);
        assert_eq!(restored.incarnation, node.incarnation);
        // last_seen is skipped in serde, should be None
        assert!(restored.last_seen.is_none());
    }

    #[test]
    fn test_node_bincode_serialization() {
        let node = Node {
            id: NodeId("bincode-test".to_string()),
            gossip_addr: "10.0.0.1:5000".parse().unwrap(),
            http_addr: "10.0.0.1:6000".parse().unwrap(),
            state: NodeState::Suspect,
            incarnation: 100,
            last_seen: None,
        };

        // Serialize with bincode (used in gossip protocol)
        let encoded = bincode::serialize(&node).expect("Bincode serialization failed");

        // Deserialize
        let restored: Node =
            bincode::deserialize(&encoded).expect("Bincode deserialization failed");

        assert_eq!(restored.id, node.id);
        assert_eq!(restored.state, NodeState::Suspect);
    }

    // ============================================================
    // GOSSIP MESSAGE TESTS
    // ============================================================

    #[test]
    fn test_gossip_ping_serialization() {
        let msg = GossipMessage::Ping {
            from: NodeId("sender-node".to_string()),
            incarnation: 5,
        };

        let encoded = bincode::serialize(&msg).expect("Failed to serialize Ping");
        let decoded: GossipMessage =
            bincode::deserialize(&encoded).expect("Failed to deserialize Ping");

        if let GossipMessage::Ping { from, incarnation } = decoded {
            assert_eq!(from.0, "sender-node");
            assert_eq!(incarnation, 5);
        } else {
            panic!("Wrong message type");
        }
    }

    #[test]
    fn test_gossip_ack_serialization() {
        let members = vec![
            Node {
                id: NodeId("node-1".to_string()),
                gossip_addr: "127.0.0.1:5000".parse().unwrap(),
                http_addr: "127.0.0.1:6000".parse().unwrap(),
                state: NodeState::Alive,
                incarnation: 1,
                last_seen: None,
            },
            Node {
                id: NodeId("node-2".to_string()),
                gossip_addr: "127.0.0.1:5001".parse().unwrap(),
                http_addr: "127.0.0.1:6001".parse().unwrap(),
                state: NodeState::Alive,
                incarnation: 2,
                last_seen: None,
            },
        ];

        let msg = GossipMessage::Ack {
            from: NodeId("responder".to_string()),
            incarnation: 10,
            members: members.clone(),
        };

        let encoded = bincode::serialize(&msg).expect("Failed to serialize Ack");
        let decoded: GossipMessage =
            bincode::deserialize(&encoded).expect("Failed to deserialize Ack");

        if let GossipMessage::Ack {
            from,
            incarnation,
            members: decoded_members,
        } = decoded
        {
            assert_eq!(from.0, "responder");
            assert_eq!(incarnation, 10);
            assert_eq!(decoded_members.len(), 2);
        } else {
            panic!("Wrong message type");
        }
    }

    #[test]
    fn test_gossip_join_serialization() {
        let node = Node {
            id: NodeId("new-joiner".to_string()),
            gossip_addr: "192.168.0.100:5000".parse().unwrap(),
            http_addr: "192.168.0.100:6000".parse().unwrap(),
            state: NodeState::Alive,
            incarnation: 1,
            last_seen: None,
        };

        let msg = GossipMessage::Join { node: node.clone() };

        let encoded = bincode::serialize(&msg).expect("Failed to serialize Join");
        let decoded: GossipMessage =
            bincode::deserialize(&encoded).expect("Failed to deserialize Join");

        if let GossipMessage::Join { node: decoded_node } = decoded {
            assert_eq!(decoded_node.id.0, "new-joiner");
        } else {
            panic!("Wrong message type");
        }
    }

    #[test]
    fn test_gossip_suspect_serialization() {
        let msg = GossipMessage::Suspect {
            node_id: NodeId("suspected-node".to_string()),
            incarnation: 15,
        };

        let encoded = bincode::serialize(&msg).expect("Failed to serialize Suspect");
        let decoded: GossipMessage =
            bincode::deserialize(&encoded).expect("Failed to deserialize Suspect");

        if let GossipMessage::Suspect {
            node_id,
            incarnation,
        } = decoded
        {
            assert_eq!(node_id.0, "suspected-node");
            assert_eq!(incarnation, 15);
        } else {
            panic!("Wrong message type");
        }
    }

    #[test]
    fn test_gossip_alive_serialization() {
        let msg = GossipMessage::Alive {
            node_id: NodeId("alive-node".to_string()),
            incarnation: 20,
        };

        let encoded = bincode::serialize(&msg).expect("Failed to serialize Alive");
        let decoded: GossipMessage =
            bincode::deserialize(&encoded).expect("Failed to deserialize Alive");

        if let GossipMessage::Alive {
            node_id,
            incarnation,
        } = decoded
        {
            assert_eq!(node_id.0, "alive-node");
            assert_eq!(incarnation, 20);
        } else {
            panic!("Wrong message type");
        }
    }

    // ============================================================
    // MEMBERSHIP SERVICE TESTS
    // ============================================================

    #[tokio::test]
    async fn test_membership_service_creation() {
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap(); // port 0 = random available
        let seed_nodes = vec![];

        let service = MembershipService::new(bind_addr, seed_nodes)
            .await
            .expect("Failed to create service");

        // Powinien mieć siebie jako członka
        assert_eq!(service.members.len(), 1);

        let alive = service.get_alive_members();
        assert_eq!(alive.len(), 1);
        assert_eq!(alive[0].id, service.local_node.id);
        assert_eq!(alive[0].state, NodeState::Alive);
    }

    #[tokio::test]
    async fn test_membership_get_member() {
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let service = MembershipService::new(bind_addr, vec![]).await.unwrap();

        // Get existing member (self)
        let member = service.get_member(&service.local_node.id);
        assert!(member.is_some());
        assert_eq!(member.unwrap().id, service.local_node.id);

        // Get non-existing member
        let fake_id = NodeId("non-existent".to_string());
        let member = service.get_member(&fake_id);
        assert!(member.is_none());
    }

    #[tokio::test]
    async fn test_membership_http_addr_calculation() {
        let bind_addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        let service = MembershipService::new(bind_addr, vec![]).await.unwrap();

        // HTTP port should be gossip port + 1000
        assert_eq!(service.local_node.gossip_addr.port(), 5000);
        assert_eq!(service.local_node.http_addr.port(), 6000);
    }

    #[tokio::test]
    async fn test_multiple_services_different_ports() {
        // Tworzymy dwa serwisy na różnych portach
        let service1 = MembershipService::new("127.0.0.1:0".parse().unwrap(), vec![])
            .await
            .unwrap();

        let service2 = MembershipService::new("127.0.0.1:0".parse().unwrap(), vec![])
            .await
            .unwrap();

        // Powinny mieć różne NodeId
        assert_ne!(service1.local_node.id, service2.local_node.id);

        // Każdy powinien mieć tylko siebie
        assert_eq!(service1.members.len(), 1);
        assert_eq!(service2.members.len(), 1);
    }

    // ============================================================
    // INCARNATION TESTS (ważne dla CRDT-like conflict resolution)
    // ============================================================

    #[test]
    fn test_incarnation_comparison() {
        // Wyższa inkarnacja wygrywa
        let node_v1 = Node {
            id: NodeId("node-x".to_string()),
            gossip_addr: "127.0.0.1:5000".parse().unwrap(),
            http_addr: "127.0.0.1:6000".parse().unwrap(),
            state: NodeState::Alive,
            incarnation: 1,
            last_seen: None,
        };

        let node_v2 = Node {
            id: NodeId("node-x".to_string()),
            gossip_addr: "127.0.0.1:5000".parse().unwrap(),
            http_addr: "127.0.0.1:6000".parse().unwrap(),
            state: NodeState::Suspect, // zmieniony state
            incarnation: 2,            // wyższa inkarnacja
            last_seen: None,
        };

        // Symulacja logiki merge - wyższa inkarnacja powinna wygrać
        assert!(node_v2.incarnation > node_v1.incarnation);
    }
}
