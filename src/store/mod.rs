pub mod memory;

pub trait KvStore {
    fn get(&self, key: &str) -> Option<String>;
    fn put(&mut self, key: &str, value: &str);
    fn delete(&mut self, key: &str);
}