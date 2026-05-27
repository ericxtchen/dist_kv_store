use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::io::AsyncReadExt;

use crate::replication::{ControllerMessage, Node, OutboundEvent};
use crate::replication::{NodeRole, ReplicationMessage, NodeHandler, LeaderRole, FollowerRole};
use std::io::{self};
use std::time::Duration;

impl Node {
    // handles on_message network type events
    pub fn handle(&mut self, msg: ReplicationMessage) -> Result<Option<ReplicationMessage>, io::Error> {
        match &mut self.role {
            NodeRole::FOLLOWER(r) => r.on_message(msg, &mut self.core),
            NodeRole::LEADER(r) => r.on_message(msg, &mut self.core)
        }
    }

    // can we really have the same function that handles on_tick the same?
    // Heartbeat is broadcast to all Follower nodes, but LeaderFailed is only broadcast to controller
    pub fn tick(&mut self) -> Result<Option<Vec<OutboundEvent>>, io::Error> {
        self.role.on_tick(&mut self.core) 
    }

    async fn broadcast(&mut self, msgs: Vec<ReplicationMessage>) -> io::Result<()>{
        // broadcast the on tick messages
        let msg = &msgs[0]; // all the heartbeats are the same, maybe just produce one so the msgs param isnt a Vec
        // should really have proper error handling...
        let encoded = postcard::to_allocvec(&msg).expect("Message failed to encode to br broadcast over TCP.");
        let len = encoded.len() as u32;
        let mut data: Vec<u8> = Vec::with_capacity(4 + encoded.len());
        data.extend_from_slice(&len.to_le_bytes());
        data.extend_from_slice(&encoded);
        
        for (addr, stream) in self.core.peer_addrs.iter_mut() {
            if let Err(err) = stream.write_all(&data).await {
                eprintln!("Failed to write to {addr}: {err}");
            }
        }

        Ok(())
    }

    // handle messages from controller to node
    pub fn handle_control(&mut self, msg: ControllerMessage) {
        match msg {
            ControllerMessage::PromoteToLeader => {
                self.role = NodeRole::LEADER(LeaderRole::new());
            }
            ControllerMessage::DemoteToFollower { new_leader_addr, new_leader_id } => {
                self.role = NodeRole::FOLLOWER(FollowerRole::new(Some(new_leader_id), Some(new_leader_addr)));
            }
            
            _ => () // LeaderFailed should be the Controller's responsibility to handle
        }
    }

    pub async fn run(&mut self) {
        let mut tick = tokio::time::interval(Duration::from_millis(150)); 
        let listener = TcpListener::bind(self.core.addr).await.unwrap();

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    if let Ok(Some(events)) = self.tick() {
                        for event in events {
                            match event {
                                OutboundEvent::ToPeers(msgs) => {
                                    self.broadcast(msgs).await;
                                }
                                OutboundEvent::ToController(status) => {
                                    self.core.tx_controller.send(status).await.ok();
                                }
                            }
                        }
                    }
                }

                Ok((mut stream, addr)) = listener.accept() => { // the whole FetchEntries -> AppendEntries -> AckOffset might not work, gotta see how back and forth communication works
                    // act on incoming ReplicationMessages
                    // deserialize the stream into a ReplicationMessage and call handle on it
                    let mut len_buf = [0u8; 4];
                    if let Err(err) = stream.read_exact(&mut len_buf).await {
                        eprintln!("read failed: {err}");
                        continue;
                    }
                        
                    let len = u32::from_le_bytes(len_buf) as usize;

                    let mut data = vec![0u8; len];
                    if let Err(err) = stream.read_exact(&mut data).await {
                        eprintln!("read failed: {err}");
                        continue;
                    }
                    let msg: ReplicationMessage =
                        match postcard::from_bytes(&data) {
                            Ok(msg) => msg,
                            Err(err) => {
                                eprintln!("bad packet from {addr}: {err}");
                                continue;
                            }
                        };

                    self.handle(msg); // should prob be async...
                }

                Some(control_msg) = self.core.rx_controller.recv() => {
                    match control_msg {
                        ControllerMessage::PromoteToLeader => {
                            self.role = NodeRole::LEADER(LeaderRole::new());
                        }
                        ControllerMessage::DemoteToFollower { new_leader_addr, new_leader_id } => {
                            self.role = NodeRole::FOLLOWER(FollowerRole::new(Some(new_leader_id), Some(new_leader_addr)));
                        }

                        _ => () // LeaderFailed should be Controller's responsibility to handle
                    }
                }
            }
        }
    }
}