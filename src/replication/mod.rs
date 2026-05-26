/*
Should have a Node class that encapsulates the ArcRwLockKvStore, which should encapsulate a WalLog.
Each Node should keep its current read offset so it knows where to read from when it needs to catch up to leader.
Should we have a global controller entity that manages failover and spawns new nodes if needed (main.rs)?
*/

use std::{collections::HashMap, os::unix::net::SocketAddr};

use uuid::Uuid;
use std::io;

use crate::{store::memory::ArcRwLockKvStore, wal::WalEntry};

pub mod leader;
pub mod replica;
pub mod transport;

pub enum ReplicationMessage {
    // handled by replica node
    AppendEntries { entries: Vec<WalEntry> },
    Heartbeat { leader_id: Uuid, leader_offset: u64 },

    // handled by leader node
    FetchEntries { replica_node_id: Uuid, from_offset: u64 },
    AckOffset { replica_node_id: Uuid, last_applied_offset: u64 },

    LeaderFailed { leader_id: Uuid }
}

enum NodeRole {
    LEADER(LeaderRole),
    FOLLOWER(FollowerRole)
}

struct NodeCore {
    id: Uuid,
    addr: SocketAddr,
    kv_store: ArcRwLockKvStore,
    last_read_offset: u64
}

struct Node {
    core: NodeCore,
    role: NodeRole
}

pub struct LeaderRole {
    replication_map: HashMap<Uuid, u64> // maps a node's uuid to the last logical offset that have seen, used to calculate how much of the leader's wal should be sent to them
}

pub struct FollowerRole {
    leader_id: Option<Uuid>,
    leader_addr: Option<SocketAddr>
}

/*
on_message is for network requests that contain a ReplicationMessage.
on_tick is for actions that are executed every interval like Heartbeat and LeaderFailed.
*/
pub trait NodeHandler {
    fn on_message(&mut self, msg: ReplicationMessage, core: &mut NodeCore) -> Result<Option<ReplicationMessage>, io::Error>;
    fn on_tick(&mut self, core: &mut NodeCore) -> Result<Option<Vec<ReplicationMessage>>, io::Error>;
}

impl Node {
    // handles on_message network type events
    pub fn handle(&mut self, msg: ReplicationMessage) -> Result<Option<ReplicationMessage>, io::Error> {
        match &mut self.role {
            NodeRole::FOLLOWER(r) => r.on_message(msg, &mut self.core),
            NodeRole::LEADER(r) => r.on_message(msg, &mut self.core)
        }
    }
}