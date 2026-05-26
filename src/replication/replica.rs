use std::io::{self, Error};

use crate::{replication::{FollowerRole, NodeCore, NodeHandler, ReplicationMessage}, wal::Wal};

impl NodeHandler for FollowerRole {
    fn on_message(&mut self, message: super::ReplicationMessage, core: &mut NodeCore) -> Result<Option<ReplicationMessage>, io::Error> {
        match message {
            ReplicationMessage::AppendEntries { entries } => {
                for wal_entry in &entries {
                    core.kv_store.wal_log.append(wal_entry);
                }

                Ok(None)
            }

            ReplicationMessage::Heartbeat { leader_id, leader_offset } => {
                // reset heartbeat timer
                Ok(None)
            }

            _ => Err(Error::new(io::ErrorKind::InvalidInput, "Invalid message type to send to Follower Node."))
        }
    }

    fn on_tick(&mut self, core: &mut NodeCore) -> Result<Option<Vec<super::ReplicationMessage>>, io::Error> {
        // if heartbeat timer drops to zero, send a LeaderFailed and trigger failover
    }
}