use std::{collections::HashSet, error::Error};

use kv_store::replication::{FollowerRole, LeaderRole, Node, NodeCore, NodeRole, ControllerMessage};
use tokio::sync::mpsc::{self, Sender, Receiver};

pub struct Controller {
    current_leader: Node,
    followers: HashSet<Node>,
    tx: Sender<ControllerMessage>,
    rx: Receiver<ControllerMessage>
}

impl Controller {
    // receive LeaderFailed and promote the follower with the highest last_applied_offset as leader
    pub async fn run(&mut self) {
         while let Some(msg) = self.rx.recv().await {
            match msg {
                ControllerMessage::LeaderFailed { leader_id} => {
                    // pick node with highest last_known_offset
                    // send ControlMessage::PromoteToLeader to it
                    // send ControlMessage::DemoteToFollower to others
                    let Some(mut new_leader) = self.followers.iter().next() else {
                        return;
                    };
                    for follower in &self.followers {
                        if follower.core.last_read_offset > new_leader.core.last_read_offset {
                            new_leader = follower;
                        }
                    }

                    new_leader.core.tx_controller.send(ControllerMessage::PromoteToLeader).await.unwrap();

                    self.followers.
                }

                _ => ()
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut handles = vec![];
    let leader_id = None;
    let leader_addr = None;
    for i in 1..=3 {
        let (tx, rx) = mpsc::channel::<ControllerMessage>(100);
        let role =  if i == 1 {
            NodeRole::LEADER(LeaderRole::new())
        } else {
            NodeRole::FOLLOWER(FollowerRole::new(leader_id, leader_addr))
        }; 

        let node = Node::new(8000 + i, role, tx, rx);

        handles.push(tokio::spawn(async move {
            node.run().await
        }));
    }

    for node in handles {
        node.await?;
    }

    Ok(())
}
