/*
Should have a Node class that encapsulates the ArcRwLockKvStore, which should encapsulate a WalLog.
Each Node should keep its current read offset so it knows where to read from when it needs to catch up to leader.
Should we have a global controller entity that manages failover and spawns new nodes if needed (main.rs)?
*/

use std::{collections::HashMap, net::{IpAddr, Ipv4Addr, SocketAddr}};

use tokio::{net::TcpStream, sync::mpsc::{Receiver, Sender}};

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::io;

use crate::{store::memory::ArcRwLockKvStore, wal::WalEntry};

pub mod leader;
pub mod replica;
pub mod transport;

// These are the messages from the Controller to the nodes
pub enum ControllerMessage {
    PromoteToLeader,
    DemoteToFollower { new_leader_id: Uuid, new_leader_addr: SocketAddr },
    LeaderFailed { leader_id: Uuid }
}

// These are the messages between nodes (ie. between leader and followers/replicas)
#[derive(Serialize, Deserialize, Debug)]
pub enum ReplicationMessage {
    // handled by replica node
    AppendEntries { entries: Vec<WalEntry> },
    Heartbeat { leader_id: Uuid, leader_offset: u64 },

    // handled by leader node
    FetchEntries { replica_node_id: Uuid, from_offset: u64 },
    AckOffset { replica_node_id: Uuid, last_applied_offset: u64 },

}

// these are the messages from the nodes to the Controller
pub enum NodeStatusMessage {
    
}

pub enum OutboundEvent {
    ToPeers(Vec<ReplicationMessage>), // heartbeat
    ToController(ControllerMessage) // leader failed
}

pub enum NodeRole {
    LEADER(LeaderRole),
    FOLLOWER(FollowerRole)
}

pub struct NodeCore {
    id: Uuid,
    addr: SocketAddr,
    kv_store: Result<ArcRwLockKvStore, std::io::Error>, // propogate up to main.rs that spawns the nodes to handle any errors
    pub last_read_offset: u64,
    pub tx_controller: Sender<ControllerMessage>,
    rx_controller: Receiver<ControllerMessage>,
    peer_addrs: HashMap<SocketAddr, TcpStream>
}

impl NodeCore {
    pub fn new(port: u16, tx_controller: Sender<ControllerMessage>, rx_controller: Receiver<ControllerMessage>, peer_addrs: HashMap<SocketAddr, TcpStream>) -> Self {
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        Self { 
            id: Uuid::new_v4(), 
            addr: SocketAddr::new(ip, port), 
            kv_store: ArcRwLockKvStore::new(), 
            last_read_offset: 0, 
            tx_controller: tx_controller,
            rx_controller: rx_controller,
            peer_addrs: peer_addrs
        }
    }
}

pub struct Node {
    pub core: NodeCore,
    pub role: NodeRole
}

pub struct LeaderRole {
    replication_map: HashMap<Uuid, u64> // maps a node's uuid to the last logical offset that have seen, used to calculate how much of the leader's wal should be sent to them
}

impl LeaderRole {
    pub fn new() -> Self {
        Self { replication_map: HashMap::new() }
    }
}

pub struct FollowerRole {
    leader_id: Option<Uuid>,
    leader_addr: Option<SocketAddr>
}

impl FollowerRole {
    pub fn new(leader_id: Option<Uuid>, leader_addr: Option<SocketAddr>) -> Self {
        Self { leader_id: leader_id, leader_addr: leader_addr }
    }
}

/*
on_message is for network requests that contain a ReplicationMessage.
on_tick is for actions that are executed every interval like Heartbeat and LeaderFailed.
*/
pub trait NodeHandler {
    fn on_message(&mut self, msg: ReplicationMessage, core: &mut NodeCore) -> Result<Option<ReplicationMessage>, io::Error>;
    fn on_tick(&mut self, core: &mut NodeCore) -> Result<Option<Vec<OutboundEvent>>, io::Error>;
}

impl Node {
    pub fn new(port: u16, role: NodeRole, tx_inter_node: Sender<ControllerMessage>, rx_inter_node: Receiver<ControllerMessage>, peer_addrs: HashMap<SocketAddr, TcpStream>) -> Self {
        Self { 
            core: NodeCore::new(port, tx_inter_node, rx_inter_node, peer_addrs), 
            role: role
        }
    }
}

impl NodeHandler for NodeRole {
    fn on_message(
        &mut self,
        msg: ReplicationMessage,
        core: &mut NodeCore,
    ) -> Result<Option<ReplicationMessage>, io::Error> {
        match self {
            NodeRole::FOLLOWER(r) => r.on_message(msg, core),
            NodeRole::LEADER(r) => r.on_message(msg, core),
        }
    }

    fn on_tick(
        &mut self,
        core: &mut NodeCore,
    ) -> Result<Option<Vec<OutboundEvent>>, io::Error> {
        match self {
            NodeRole::FOLLOWER(r) => r.on_tick(core),
            NodeRole::LEADER(r) => r.on_tick(core),
        }
    }
}