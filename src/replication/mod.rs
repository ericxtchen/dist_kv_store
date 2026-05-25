/*
Should have a Node class that encapsulates the ArcRwLockKvStore, which should encapsulate a WalLog.
Each Node should keep its current read offset so it knows where to read from when it needs to catch up to leader.
*/

use crate::store::memory::ArcRwLockKvStore;

pub enum NodeRole {
    LEADER,
    FOLLOWER
}

pub struct Node {
    last_read_offset: u64,
    kv_store: ArcRwLockKvStore
}