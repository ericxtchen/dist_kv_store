use std::{collections::HashMap, fs::{File, OpenOptions}, io::{self, BufReader, Read, Seek, Write}, path::PathBuf};

use crate::wal::{Wal, WalEntry};

pub struct WalLog {
    wal_path: PathBuf,
    file: File,
    index: HashMap<u64, u64> // hashmap to map logical offset to byte offset in wal file
}

impl WalLog {
    pub fn new(path: &str) -> io::Result<Self> {
        let wal_path_ = PathBuf::from(path);
        let file = Self::create_if_missing(&wal_path_)?;
        let index = Self::build_index(&file)?;

        Ok(Self {
            wal_path: wal_path_, 
            file: file,
            index: index
        })
    }

    fn create_if_missing(path: &PathBuf) -> io::Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        
        Ok(file)
    }

    fn build_index(file: &File) -> io::Result<HashMap<u64, u64>> {
        let mut reader = BufReader::new(file);
        let mut map = HashMap::<u64, u64>::new();
        let mut byte_pos: u64 = 0;
        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // clean end of file
                Err(e) => return Err(e),
            }

            let len = u32::from_le_bytes(len_buf) as usize;

            let mut rest = vec![0u8; 4 + len];
            reader.read_exact(&mut rest)?;

            let entry: WalEntry = postcard::from_bytes(&rest[4..]).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e) )?;

            map.insert(entry.offset, byte_pos);
            byte_pos += 4 + 4 + len as u64;
        }

        Ok(map)
    }

    pub fn get_byte_pos(&self, offset: u64) -> Option<&u64> {
        self.index.get(&offset)
    }

    pub fn get_entries(&mut self, offset: u64) -> io::Result<Vec<WalEntry>> {
        self.read(offset)
    }
}

impl Wal for WalLog {
    fn append(&mut self, entry: &WalEntry) -> io::Result<()> {
        let byte_pos = self.file.seek(io::SeekFrom::End(0))?;
        let encoded = postcard::to_allocvec(entry)
            .expect("Serialization should not fail.");
        let len = encoded.len() as u32;
        let checksum = crc32fast::hash(&encoded);

        let mut buf = Vec::with_capacity(8 + encoded.len());
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf.extend_from_slice(&encoded);

        self.file.write_all(&buf)?;
        self.file.sync_data()?;

        self.index.insert(entry.offset, byte_pos);
        Ok(())
    }

    fn read(&mut self, offset: u64) -> io::Result<Vec<WalEntry>> {
        let byte_pos = self.index.get(&offset)
            .copied()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "offset not found"))?;
        self.file.seek(io::SeekFrom::Start(byte_pos))?;
        let mut reader = BufReader::new(&self.file);
        let mut data:Vec<WalEntry> = Vec::new();


        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // clean end of file
                Err(e) => return Err(e),
            }

            let len = u32::from_le_bytes(len_buf) as usize;

            let mut checksum_buf = [0u8; 4];
            reader.read_exact(&mut checksum_buf)?;
            let expected_crc = u32::from_le_bytes(checksum_buf);

            let mut data_ = vec![0u8; len];
            reader.read_exact(&mut data_)?;

            let actual_crc = crc32fast::hash(&data_);
            if expected_crc != actual_crc {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC32 Hash doesn't match. Data may be corrupted."))
            }

            let entry: WalEntry = postcard::from_bytes(&data_).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e) )?;

            data.push(entry);
        }

        Ok(data)
    }
}