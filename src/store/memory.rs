use std::{collections::HashMap, io, sync::{Arc, RwLock}};
use crate::wal::writer::WalLog;

use super::KvStore;

pub struct ArcRwLockKvStore {
    data: Arc<RwLock<HashMap<String, String>>>,
    wal_log: WalLog
}

impl ArcRwLockKvStore {
    fn new() -> io::Result<Self> {
        Ok(Self { 
            data: Arc::new(RwLock::new(HashMap::new())),
            wal_log: WalLog::new("/tmp/log/wal_log")?
        })
    }
}

impl KvStore for ArcRwLockKvStore {
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