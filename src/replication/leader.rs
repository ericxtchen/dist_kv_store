use std::io::{self, Error};

use crate::replication::{LeaderRole, NodeHandler, OutboundEvent};

impl NodeHandler for LeaderRole {
    fn on_message(&mut self, msg: super::ReplicationMessage, core: &mut super::NodeCore) -> Result<Option<super::ReplicationMessage>, std::io::Error> {
        match msg {
            super::ReplicationMessage::FetchEntries { replica_node_id, from_offset } => {
                let byte_pos = core.kv_store
                    .get_byte_pos(from_offset)
                    .ok_or_else(|| { Error::new(io::ErrorKind::NotFound, "Byte offset not found from WAL logical offset.")})?;
                let entries = core.kv_store.wal_log.get_entries(*byte_pos)?;

                Ok(Some(super::ReplicationMessage::AppendEntries { entries: entries }))
            }

            super::ReplicationMessage::AckOffset { replica_node_id, last_applied_offset } => {
                self.replication_map.insert(replica_node_id, last_applied_offset);
                Ok(None)
            }

            _ => Err(Error::new(io::ErrorKind::InvalidInput, "Invalid message type to send to Leader Node."))
        }

    }
    // probably some async tokio event loop somewhere else that calls this every 30 or so seconds
    // should we include the replica id or addr in these enums...?
    fn on_tick(&mut self, core: &mut super::NodeCore) -> Result<Option<Vec<super::OutboundEvent>>, io::Error> {
        // produce HeartBeat and push new WAL entries to replicas
        let mut ret = vec![];
        for (replica_node_id, last_written_offset) in &self.replication_map {
            ret.push(super::ReplicationMessage::Heartbeat { leader_id: core.id, leader_offset: core.last_read_offset });
            
            if (last_written_offset < &core.last_read_offset) {
                let entries = core.kv_store.wal_log.get_entries(*last_written_offset)?;
                ret.push(super::ReplicationMessage::AppendEntries { entries: entries })
            } // maybe we have to push an empty AppendEntries too, depends on how the follower will parse it
              // and how we know to give the right heartbeat response to whom
              // is a vector of these replication messages the right way? ig depends on how messages will be sent and received
        }

        Ok(Some(vec![OutboundEvent::ToPeers(ret)]))
    }
}