pub mod writer;
pub mod operation;

use std::io;

use serde::{Deserialize, Serialize};

use crate::wal::operation::Operation;

/*
We need an offset or index number as an id for the action that took place.
And then we need something that actually represents the action taken place (put/delete).
Gets probably aren't necessary to log because they do not modify the hashmap.
Each replica/leader node should also store which offset it has just read/knows
*/
#[derive(Serialize, Deserialize, Debug)]
pub struct WalEntry {
    offset: u64, // this should always increase
    operation: Operation,
    key: String,
    value: String
}

pub trait Wal {
    fn append(&mut self, entry: &WalEntry) -> io::Result<()>;
    fn read(&mut self, offset: u64) -> io::Result<Vec<WalEntry>>; // given an offset number, start reading the events from that offset number continually to the end
}
