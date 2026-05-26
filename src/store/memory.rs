use std::{collections::HashMap, io, sync::{Arc, RwLock}};
use crate::wal::writer::WalLog;

use super::KvStore;

pub struct ArcRwLockKvStore {
    data: Arc<RwLock<HashMap<String, String>>>,
    pub wal_log: WalLog
}

impl ArcRwLockKvStore {
    pub fn new() -> io::Result<Self> {
        Ok(Self { 
            data: Arc::new(RwLock::new(HashMap::new())),
            wal_log: WalLog::new("/tmp/log/wal_log")?
        })
    }

    pub fn get_byte_pos(&self, offset: u64) -> Option<&u64> {
        self.wal_log.get_byte_pos(offset)
    }
}

impl KvStore for ArcRwLockKvStore { // implement writing to WAL first and then writing to kv store
    fn get(&self, key: &str) -> Option<String> {
        let read_guard = self.data.read().unwrap();
        return read_guard.get(key).cloned();
    }

    fn put(&mut self, key: &str, value: &str) {
        let mut write_guard = self.data.write().unwrap();
        write_guard.insert(key.to_string(), value.to_string());
    }

    fn delete(&mut self, key: &str) {
        let mut write_guard = self.data.write().unwrap();
        write_guard.remove(key);
    }
}