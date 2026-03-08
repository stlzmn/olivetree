use anyhow::Result;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Instant;
use std::{net::SocketAddr, time::Duration};
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tracing::info;

use super::types::{GossipMessage, Node, NodeId, NodeState};

const GOSSIP_INTERVAL: Duration = Duration::from_millis(500);
const FAILURE_DETECTION_INTERVAL: Duration = Duration::from_secs(2);
const SUSPECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEAD_TIMEOUT: Duration = Duration::from_secs(10);

/// Background membership service using UDP gossip.
///
/// It tracks known cluster nodes and their liveness states, and provides
/// helper methods used by partitioning and replication layers.
pub struct MembershipService {
    /// Descriptor of the local node (self).
    pub local_node: Node, // description of local node, the "SELF"
    /// Current local view of known cluster members.
    pub members: Arc<DashMap<NodeId, Node>>, // map of the other members of the cluster, so to know
    // where to send requests and stuff
    socket: Arc<UdpSocket>, // udp socket, a meant to make any network call regarding membership
    // affairs, just the lowest level stuff in our code. GOSSIP + SWIM
    incarnation: Arc<RwLock<u64>>, // the "version of the node" - used to resolve messages
                                   // conflicts after temporary node's dead. Newest wins.
}

impl MembershipService {
    /// Creates a new membership service bound to `bind_addr`.
    ///
    /// If `seed_nodes` is not empty, the service sends a `Join` message
    /// to each seed node during initialization.
    pub async fn new(bind_addr: SocketAddr, seed_nodes: Vec<SocketAddr>) -> Result<Arc<Self>> {
        let socket = UdpSocket::bind(bind_addr).await?; // creation of the udp socket, async op so
        // we await not to block other stuff,
        let incarnation_counter = Arc::new(RwLock::new(1)); // initial incarnation is set to 1
        let current_inc = *incarnation_counter.read().await;
        let local_node = Node {
            // info about ourselves
            id: NodeId::new(), // we need ofc node id to identify and be indentified, uuid provides
            // 99.9999% chances for colisoin free ids
            gossip_addr: bind_addr, // gossip address used for udp communication to find out who is
            // alive and whos not
            http_addr: SocketAddr::new(bind_addr.ip(), bind_addr.port() + 1000), // http address
            // used just for
            // communication
            // between nodes
            // in regard of
            // replication of
            // the data etc
            state: NodeState::Alive, // initial state of the node is ofc alive
            incarnation: current_inc, // incarnation is 1
            last_seen: Some(Instant::now()), // last seen time to find out the node is
                                     // alive/suspect/dead
        };
        let members = Arc::new(DashMap::new()); // every node stores info about other nodes
        members.insert(local_node.id.clone(), local_node.clone()); // we insert ourselves, we are
        // sure we are alive because we
        // "saw" ourselves
        if !seed_nodes.is_empty() {
            //if seed nodes is empty, it means nobody is already in the
            //cluster, we are the first otherwise we connect to already
            //exsiting member of the cluster
            info!("Joining cluster via {} seed node(s)", seed_nodes.len());
            // interting through the nodes, (borrow) and send them Join message so they can note
            // our presence
            for seed_node in seed_nodes.iter() {
                let msg = GossipMessage::Join {
                    node: local_node.clone(),
                };
                // encoding message makes it possible to lightweightly send it through the udp
                // socket
                let encoded = bincode::serialize(&msg)?;
                socket.send_to(&encoded, seed_node).await?;
                info!("Sent join request to {}", seed_node);
            }
        }
        // Successfully created all and packet do Arc to be easily "copied" by reference to "trick"
        // borrow checker and use it in multiple places with ownership needed
        Ok(Arc::new(Self {
            local_node,
            members,
            socket: Arc::new(socket),
            incarnation: incarnation_counter,
        }))
    }
    /// Starts background gossip, receive and failure-detection loops.
    pub async fn start(self: Arc<Self>) {
        tracing::info!("Starting membership service...");

        // loop for pinging random members of the cluster, essential for presence checking and
        // updating the data
        let _gossip_handle = {
            let service = self.clone(); // we need to clone our service so we can "eat" it inside
            // the async task that can possibly run longer that context
            // used to run it
            tokio::spawn(async move {
                service.gossip_loop().await;
            })
        };

        // loop for receiving messages, all other gossip messages are handled here
        let _receive_handle = {
            let service = self.clone();
            tokio::spawn(async move {
                service.receive_loop().await;
            })
        };

        // this task monitors the last seen, and suspect nodes to state who is dead and who is not
        let _failure_detection_handle = {
            let service = self.clone();
            tokio::spawn(async move {
                service.failure_detection_loop().await;
            })
        };

        tracing::info!("All background tasks started");
    }

    /// Returns one member by node id, if present in the local view.
    pub fn get_member(&self, node_id: &NodeId) -> Option<Node> {
        self.members.get(node_id).map(|entry| entry.clone())
    }

    /// Returns all members currently marked as [`NodeState::Alive`].
    pub fn get_alive_members(&self) -> Vec<Node> {
        self.members
            .iter() // iter by the cluster members (DashMap, so we have key and value)
            .filter(|entry| entry.value().state == NodeState::Alive) // filter this iterator and
            // reaturn only state == Alive
            .map(|entry| entry.value().clone()) // map the (key, value) -> value
            .collect() // consume the iterator to get the vecotr and return immediately
    }

    // used to send the ping to random alive cluster member to initiate communication therefore
    // data exchange
    async fn gossip_loop(self: Arc<Self>) {
        let mut interval = tokio::time::interval(GOSSIP_INTERVAL); // we set the inteval of loop
        // tick

        loop {
            interval.tick().await; // we need to use async "time tick" to get the exact time tick
            // adjusted to some internal shit inside async runtime, normal
            // wouldnt give us exact GOSSIP_INTERVAL tick

            let alive_members: Vec<Node> = self // we collect alive members to know where to choose
                // from random recipient
                .members
                .iter()
                .filter(|entry| {
                    entry.value().id != self.local_node.id // we dont want to send to ourselves
                        && entry.value().state == NodeState::Alive
                })
                .map(|entry| entry.value().clone())
                .collect();

            // skip if theres none alive members, we dont want to waste cpu for nothing
            if alive_members.is_empty() {
                continue;
            }

            use rand::Rng;
            let idx = rand::thread_rng().gen_range(0..alive_members.len()); // 0..alive_members.len()
            // is exclusive so
            // alive_members.len()
            // does the job and no
            // need for ... -1 shit
            let target = &alive_members[idx];
            // ^^^ random choose of the recipient of our ping message

            let incarnation = *self.incarnation.read().await; // we need to take our incarnation to
            // send it to recipient to let him
            // know our age
            let msg = GossipMessage::Ping {
                // Ping message preparation
                from: self.local_node.id.clone(), // we just send our id
                incarnation, // and incarnation, they will note that we are alive, respond us with
                             // update last seen
                             // other nodes he knows about, and if needed update incarnation
            };

            // if encoding of the abovementioned message, we go forward
            if let Ok(encoded) = bincode::serialize(&msg) {
                if let Err(e) = self.socket.send_to(&encoded, target.gossip_addr).await {
                    // send
                    // to can
                    // fail
                    // so we
                    // check
                    // it as
                    // well
                    tracing::warn!("Failed to send ping to {:?}: {}", target.id, e);
                } else {
                    tracing::debug!("Sent ping to {:?}", target.id);
                }
            } else {
                // if encoding failed, we dont have a shit to send so we just do nothing and
                // "print" error
                tracing::error!("Failed to serialize GossipMessage::Ping");
            }
        }
    }

    // task for receiving messages
    async fn receive_loop(self: Arc<Self>) {
        let mut buf = vec![0u8; 65536]; // max size of package (datagram?) in udp communication,
        // our messages are far smaller but its better to have
        // margin because you never know

        loop {
            match self.socket.recv_from(&mut buf).await {
                // receive as soon as sth comes from
                // recv from returns lenght of received data and the package itself
                Ok((len, src)) => match bincode::deserialize::<GossipMessage>(&buf[..len]) {
                    // we
                    // deserialize
                    // what
                    // had
                    // come
                    // and
                    Ok(msg) => {
                        // if successfully have GossipMessage (enum)
                        if let Err(e) = self.handle_message(msg, src).await {
                            // try to handle it
                            tracing::error!("Error handling message from {}: {}", src, e);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to deserialize message from {}: {}", src, e);
                    }
                },
                Err(e) => {
                    tracing::error!("Failed to receive UDP packet: {}", e);
                    tokio::time::sleep(Duration::from_millis(100)).await; // failure in receiving
                    // udp packet, maybe
                    // needed to adjust
                    // waiting time,
                }
            }
        }
    }

    // handling all gossip messages that node can receive
    async fn handle_message(&self, msg: GossipMessage, src: SocketAddr) -> Result<()> {
        match msg {
            // simple matching what we got
            GossipMessage::Ping { from, incarnation } => {
                self.handle_ping(from, incarnation, src).await?;
            }

            GossipMessage::Ack {
                from,
                incarnation,
                members,
            } => {
                self.handle_ack(from, incarnation, members).await?;
            }

            GossipMessage::Join { node } => {
                self.handle_join(node).await?;
            }

            GossipMessage::Suspect {
                node_id,
                incarnation,
            } => {
                self.handle_suspect(node_id, incarnation).await?;
            }

            GossipMessage::Alive {
                node_id,
                incarnation,
            } => {
                self.handle_alive(node_id, incarnation).await?;
            }
        }

        Ok(())
    }

    // what to do with ping message
    async fn handle_ping(
        &self,
        from: NodeId, // we need ofc to know who sent us this ping to update the info about this
        // node
        from_incarnation: u64, // also needed to know wether the information is expired or not
        src: SocketAddr,       // address to reply to
    ) -> Result<()> {
        tracing::debug!("Received ping from {:?}", from);

        // if we got this guy on the "list" we procceed as follows
        if let Some(mut member) = self.members.get_mut(&from) {
            member.last_seen = Some(Instant::now()); // last seen right now because we see him so
            // it means he's alive

            if from_incarnation > member.incarnation {
                // if the incarnation is greater than ours
                // we update to it because node sent us this
                // ping so it means that his info is
                // freshest
                member.incarnation = from_incarnation;
            }
        } else {
            // what we do when this guy is not in our list
            tracing::info!("Discovered new member via ping: {:?} at {}", from, src);
            // well, he pinged us so he must be cluster member we dont know aobut, and he is alive
            // since he can "speak"
            let new_node = Node {
                id: from.clone(), // we create him and put to our map
                gossip_addr: src,
                http_addr: SocketAddr::new(src.ip(), src.port() + 1000),
                state: NodeState::Alive,
                incarnation: from_incarnation,
                last_seen: Some(Instant::now()),
            };

            self.members.insert(new_node.id.clone(), new_node);
        }

        let all_members: Vec<Node> = self
            .members
            .iter()
            .map(|entry| entry.value().clone())
            .collect(); // we get the list of the cluster nodes from our list to give it to the
        // node that pinged us, we want to keep him informed
        let my_incarnation = *self.incarnation.read().await; // as well as our incarnation to make
        // him sure the info is fresh
        let reply = GossipMessage::Ack {
            // we compose the Ack message
            from: self.local_node.id.clone(), // our node is
            incarnation: my_incarnation,      // incarnation
            members: all_members,             // and all members
        };

        let encoded = bincode::serialize(&reply)?; // we encode it to make it nicely prepared for
        // udp send
        self.socket.send_to(&encoded, src).await?; // we try to send, ? means that in case of
        // failure stop here and propagate the error
        // atop the function to be retuned from it

        tracing::debug!("Sent ack to {:?} with {} members", from, self.members.len()); // once we
        // did to
        // here it
        // means
        // that all
        // went okay

        Ok(())
    }

    // waht we do if we receive the ack message?
    async fn handle_ack(
        &self,
        from: NodeId,          // who sent it?
        from_incarnation: u64, // how "fresh" is he?
        members: Vec<Node>,    // and what info does he have
    ) -> Result<()> {
        tracing::debug!(
            "Received ack from {:?} (inc={}) with {} members",
            from,
            from_incarnation,
            members.len()
        );
        tracing::debug!("MEMBERS: {:?}", members);

        // if ack has been sent by the node we know exists
        if let Some(mut member) = self.members.get_mut(&from) {
            if member.state == NodeState::Dead {
                // but the node is marked as dead
                return Ok(()); // we do nothing, we dont care about info from dead nodes, dead
                // dead nodes can send messages because the info can be not propagated already,
            }

            member.last_seen = Some(Instant::now()); // if alive we update time we seen it for
            // right a moment ago, (right now)

            if from_incarnation > member.incarnation {
                member.incarnation = from_incarnation; // if he speaks it means that his info from
                // him is freshest possible
            }
        }

        // we need to update (merege) our knowledge aobut other cluster members the node sending
        // ack told us about
        for member in members {
            self.merge_member(member).await;
        }

        Ok(())
    }

    // we "merge" the info about other members, to have consistency across the cluster
    async fn merge_member(&self, new_member: Node) {
        match self.members.get_mut(&new_member.id) {
            // we get mutable ref to the one of the
            // members
            Some(mut existing) => {
                // if we got him
                if existing.state == NodeState::Dead {
                    // and he's dead
                    tracing::debug!("Ignoring update for dead node {:?}", existing.id); // we
                    // inform
                    // and do
                    // nothing

                    return;
                }
                // if not dead
                if new_member.incarnation > existing.incarnation {
                    // and freshest than ours
                    tracing::debug!(
                        "Updating {:?}: inc {} -> {}",
                        new_member.id,
                        existing.incarnation,
                        new_member.incarnation,
                    );

                    // we update because freshest info is the real info
                    existing.state = new_member.state;
                    existing.incarnation = new_member.incarnation;
                    existing.last_seen = Some(Instant::now()); // btw we dont send last seen
                    // because instant is not
                    // serializable and its difficult to
                    // send via udp
                }
            }
            None => {
                // if we dont have the node on out list
                if new_member.state == NodeState::Dead {
                    return; // and he's dead we do nothing
                }
                tracing::info!(
                    "Doscovered new member: {:?} at {}",
                    new_member.id,
                    new_member.gossip_addr
                );
                // otherwise we know its somebody we havent seen before

                let mut member_with_timestamp = new_member; // we want him
                member_with_timestamp.last_seen = Some(Instant::now()); // just in case we update
                // its last seen data

                self.members // and simply put him on our list
                    .insert(member_with_timestamp.id.clone(), member_with_timestamp);
            }
        }
    }

    // what to do if we see message saying that some of the members is suspected to be dead
    async fn handle_suspect(&self, node_id: NodeId, incarnation: u64) -> Result<()> {
        let msg_to_broadcast = {
            // trick overcome borrow checker, a list of messages to be sent to
            // who
            match self.members.get_mut(&node_id) {
                // we take this suspected guy for and inspection
                Some(mut existing) => {
                    if existing.state == NodeState::Dead {
                        // if we have him on list but he's dead
                        // we just ignore this info
                        tracing::debug!("Ignoring Alive message for dead node {:?}", node_id);
                        return Ok(());
                    }
                    if incarnation > existing.incarnation {
                        // if the info is freshest than ours we
                        // need to treat it as a actual state
                        // of facts
                        if existing.id == self.local_node.id {
                            // if this suspected node is
                            // happened to be us
                            tracing::info!(
                                "Self-defense: Node {:?} at {} - refuting suspiction",
                                node_id,
                                self.local_node.gossip_addr
                            );
                            // we just do self defese, we know we are alive, so we can
                            let my_incarnation = {
                                let mut inc = self.incarnation.write().await;
                                *inc += 1;
                                *inc
                            }; // increase our incarnation

                            existing.incarnation = my_incarnation; // update it
                            existing.state = NodeState::Alive; // put our state as Alive
                            existing.last_seen = Some(Instant::now()); // update last seen

                            Some(GossipMessage::Alive {
                                // and prepare Gossip Alive message to
                                // inform all members (broadcast message)
                                // about our fresh whereabouts
                                node_id: node_id.clone(),
                                incarnation: my_incarnation,
                            })
                        } else {
                            tracing::info!(
                                "Node {:?} at {} suspected",
                                existing.id,
                                existing.gossip_addr
                            );
                            existing.state = NodeState::Suspect; // if the guy is somebody else
                            // than we, we just mark him as
                            // Suspected, we update our list
                            // info
                            existing.incarnation = incarnation; // according to freshest
                            // existing.last_seen = Some(Instant::now()); // and last seen now,
                            // because we see him? good
                            // point, we actually dont
                            // see him so its not
                            // properly to se it here i
                            // guess, to be confirmed

                            None
                        }
                    } else {
                        None
                    }
                }
                None => {
                    tracing::debug!("Suspected node {:?} doesn't exist", node_id);
                    None
                }
            }
        };

        if let Some(msg) = msg_to_broadcast {
            self.broadcast_message(msg).await; // after freeing the lock on members dash map we can
            // easily use broadcast_message wihtout deadlock (it
            // also uses members)
        }
        Ok(())
    }

    // what if we got a message saying that somebody is alive?
    async fn handle_alive(&self, node_id: NodeId, incarnation: u64) -> Result<()> {
        match self.members.get_mut(&node_id) {
            // same thing as before, we check if we know this
            // fella
            Some(mut existing) => {
                // if we do
                if existing.state == NodeState::Dead {
                    // we check if he's not cold
                    tracing::debug!("Ignoring Alive message for dead node {:?}", node_id);
                    return Ok(());
                }
                if incarnation > existing.incarnation {
                    // if he's shouting then he's alive so we
                    // just check if he is propagating
                    // fresh info
                    tracing::info!(
                        "Node {:?} at {} is now Alive (inc={})",
                        existing.id,
                        existing.gossip_addr,
                        incarnation
                    );
                    // if yes we believe him and we update out knowledge according to "what he says"
                    existing.state = NodeState::Alive;
                    existing.incarnation = incarnation;
                    existing.last_seen = Some(Instant::now());
                } else if incarnation == existing.incarnation
                    && existing.state == NodeState::Suspect
                // if the info "freshness" seems to be
                // as fresh as ours but the we have him
                // as a suspect we need to mark him
                // alive because if he were suspected
                // to be dead he wouldn't say a word
                {
                    tracing::info!(
                        "Node {:?} at {} successfully refuted suspiction",
                        existing.id,
                        existing.gossip_addr,
                    );
                    existing.state = NodeState::Alive;
                    // existing.incarnation = incarnation; // makes no sense becase his incarnation is
                    // == to the one we knew was
                    existing.last_seen = Some(Instant::now()); // this is okay, we see him moving
                    // so we saw him recently living
                }
            }
            None => {
                tracing::debug!("Alive message for unknown node {:?}", node_id);
            }
        }

        Ok(())
    }

    // we need to know what to do when new guy joins the circle
    async fn handle_join(&self, mut node: Node) -> Result<()> {
        tracing::info!("Node {:?} joining cluster at {}", node.id, node.gossip_addr);

        node.last_seen = Some(Instant::now()); // we just accept him as our firend, we just mark
        // that we seen him, only nodes at start sent the
        // join message so they will never be on the list of
        // none node, (really?)

        self.members.insert(node.id.clone(), node.clone()); // we insert him without any questions

        tracing::info!("Cluster size now: {}", self.members.len());

        Ok(())
    }

    // we look here for any suspect and what do with them when they are dead
    async fn failure_detection_loop(self: Arc<Self>) {
        let mut interval = tokio::time::interval(FAILURE_DETECTION_INTERVAL); // how frequent we
        // look for the
        // expired ones

        loop {
            interval.tick().await;
            let now = Instant::now(); // needet to say how much time passed from last seen of the
            // node

            let mut messages_to_broadcast = Vec::new();

            for mut entry in self.members.iter_mut() {
                // we need to inspect all of the members
                // from the cluster
                let member = entry.value_mut(); // we need it mutble so we can modify the stte if
                // needed

                if member.id == self.local_node.id {
                    // we know we are alive so we will for sure
                    // not be dead or suspect
                    continue;
                }

                if member.state == NodeState::Dead {
                    // what if node is already dead? we skip shit
                    continue;
                }

                if let Some(last_seen) = member.last_seen {
                    // if the member was seen at some point
                    // in the past
                    let elapsed = now.duration_since(last_seen); // we want to know how long ago
                    // was it

                    match member.state {
                        // we look now what state this node is in
                        NodeState::Alive => {
                            // if hes alive
                            if elapsed > SUSPECT_TIMEOUT {
                                // but the time indicates that he is
                                // suspect
                                tracing::warn!(
                                    "Node {:?} suspected (no contact fo {:?})",
                                    member.id,
                                    elapsed
                                );

                                member.state = NodeState::Suspect; // we mark him as a suspect

                                let msg = GossipMessage::Suspect {
                                    node_id: member.id.clone(),
                                    incarnation: member.incarnation,
                                }; // and send the message to other nodes

                                messages_to_broadcast.push(msg); // acutally should we? or we just
                                // should leave it to be
                                // propagated by ping <-> ack
                                // mechanism ???
                            }
                        }

                        NodeState::Suspect => {
                            // if the guy is suspected
                            if elapsed > DEAD_TIMEOUT {
                                // and the time for him looks that he's
                                // dead
                                tracing::debug!(
                                    "Node {:?} declared DEAD (no contact for {:?})",
                                    member.id,
                                    elapsed
                                );

                                member.state = NodeState::Dead; // we state him as being dead, and
                                // the info later will propagate by
                                // ping <-> ack
                            }
                        }

                        NodeState::Dead => {} // we do nothing about dead nodes, they will not be
                                              // recovered, we sould add periodicall garbage
                                              // collection of dead nodes as well
                    }
                } else {
                    member.last_seen = Some(now); // if the node doesnt have last seen we just
                    // state that we see him right now
                }
            }

            for msg in messages_to_broadcast {
                // we send the messages here because the lock is
                // removed from the dashmap with members, rust
                // ownership related stuff
                self.broadcast_message(msg).await;
            }
        }
    }

    // broadcasting message to all alive nodes
    async fn broadcast_message(&self, msg: GossipMessage) {
        if let Ok(encoded) = bincode::serialize(&msg) {
            // if we could encode the message
            for entry in self.members.iter() {
                // we interate through every member of our cluster
                // we know about
                let member = entry.value(); // we just care about it's value, we don't want the key
                // for this opeartion

                if member.id == self.local_node.id {
                    // we skip ourselves because we dont want to
                    // sent the message to ourserlves we continue
                    continue;
                }

                if member.state == NodeState::Alive // if he's alive
                    && let Err(e) = self.socket.send_to(&encoded, member.gossip_addr).await
                // we
                // try
                // to
                // send
                // the
                // message
                // to
                // him
                {
                    tracing::warn!("Failed to broadcast to {:?}: {}", member.id, e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_membership_creation() {
        let bind_addr = "127.0.0.1:0".parse().unwrap();
        let seed_nodes = vec![];

        let service = MembershipService::new(bind_addr, seed_nodes)
            .await
            .expect("Failed to create service");

        assert_eq!(service.members.len(), 1);

        let members = service.get_alive_members();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].state, NodeState::Alive);

        println!("Test passed! Local node: {:?}", service.local_node.id);
    }
}
