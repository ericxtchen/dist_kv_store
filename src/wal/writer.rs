use std::{fs::{File, OpenOptions}, io::{self, BufReader, Read, Seek, Write}, path::PathBuf};

use crate::wal::{Wal, WalEntry};

pub struct WalLog {
    wal_path: PathBuf,
    file: File
}

impl WalLog {
    fn new(path: &str) -> io::Result<Self> {
        let wal_path_ = PathBuf::from(path);
        let file = Self::create_if_missing(&wal_path_)?;

        Ok(Self {wal_path: wal_path_, file: file})
    }

    fn create_if_missing(path: &PathBuf) -> io::Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        
        Ok(file)
    }
}

impl Wal for WalLog {
    fn append(&mut self, entry: &WalEntry) -> io::Result<()> {
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
        Ok(())
    }

    fn read(&mut self, offset: u64) -> io::Result<Vec<WalEntry>> {
        self.file.seek(io::SeekFrom::Start(offset))?;
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