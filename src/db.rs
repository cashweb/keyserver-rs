use std::sync::Arc;

use prost::Message;
use rocksdb::{Error as RocksError, Options, DB};

use crate::models::{database::DatabaseWrapper, keyserver::Peers};

const METADATA_NAMESPACE: u8 = b'm';
const PEER_NAMESPACE: u8 = b'p';

#[derive(Clone)]
pub struct Database(Arc<DB>);

impl Database {
    pub fn try_new(path: &str) -> Result<Self, RocksError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        DB::open(&opts, &path).map(Arc::new).map(Database)
    }

    /// Get raw `DatabaseWrapper` from the database.
    pub fn get_raw_metadata(&self, addr: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        let key = [&[METADATA_NAMESPACE], addr].concat();
        self.0.get(key)
    }

    /// Get a `DatabaseWrapper` from the database.
    pub fn get_metadata(&self, addr: &[u8]) -> Result<Option<DatabaseWrapper>, RocksError> {
        self.get_raw_metadata(addr).map(|raw_opt| {
            raw_opt.map(|raw| {
                DatabaseWrapper::decode(&raw[..]).unwrap() // This panics if stored bytes are malformed
            })
        })
    }

    /// Put a serialized `DatabaseWrapper` to the database.
    pub fn put_metadata(&self, addr: &[u8], raw: &[u8]) -> Result<(), RocksError> {
        // Prefix key
        let key = [&[METADATA_NAMESPACE], addr].concat();

        self.0.put(key, raw)
    }

    /// Get `Peers` from database.
    pub fn get_peers(&self) -> Result<Option<Peers>, RocksError> {
        self.get_peers_raw().map(|raw_peers_opt| {
            raw_peers_opt.map(|raw_metadata| {
                Peers::decode(&raw_metadata[..]).unwrap() // This panics if stored bytes are malformed
            })
        })
    }

    /// Get serialized `Peers` from database.
    pub fn get_peers_raw(&self) -> Result<Option<Vec<u8>>, RocksError> {
        self.0.get([PEER_NAMESPACE])
    }

    /// Put serialized `Peers` to database.
    pub fn put_peers(&self, raw: &[u8]) -> Result<(), RocksError> {
        self.0.put([PEER_NAMESPACE], raw)
    }
}

#[cfg(test)]
pub mod tests {
    use rocksdb::{Options, DB};

    use super::*;
    use crate::models::{
        database::DatabaseWrapper,
        keyserver::{Peer, Peers},
    };

    #[test]
    fn peers() {
        const TEST_NAME: &str = "./tests/peer";

        // Create database
        let database = Database::try_new(TEST_NAME).unwrap();

        // Create peers
        let peer_a = Peer {
            url: "url a".to_string(),
        };
        let peer_b = Peer {
            url: "url b".to_string(),
        };
        let peers_in = Peers {
            peers: vec![peer_a, peer_b],
        };
        let mut peers_raw = Vec::with_capacity(peers_in.encoded_len());
        peers_in.encode(&mut peers_raw).unwrap();

        // Put to database
        database.put_peers(&peers_raw).unwrap();

        // Get from database
        let peers_out = database.get_peers().unwrap().unwrap();
        assert_eq!(peers_in, peers_out);

        // Destroy database
        drop(database);
        DB::destroy(&Options::default(), TEST_NAME).unwrap();
    }

    #[test]
    fn metadata() {
        const TEST_NAME: &str = "./tests/metadata";

        // Create database
        let database = Database::try_new(TEST_NAME).unwrap();

        // Create database wrapper
        let database_wrapper_in = DatabaseWrapper {
            token: vec![0, 1, 3, 4],
            serialized_auth_wrapper: vec![2, 3, 4],
        };
        let mut database_wrapper_raw = Vec::with_capacity(database_wrapper_in.encoded_len());
        database_wrapper_in
            .encode(&mut database_wrapper_raw)
            .unwrap();

        // Put to database
        let addr = vec![0, 3, 4, 3, 2];
        database.put_metadata(&addr, &database_wrapper_raw).unwrap();

        // Get from database
        let data_wrapper_out = database.get_metadata(&addr).unwrap().unwrap();
        assert_eq!(database_wrapper_in, data_wrapper_out);

        // Destroy database
        drop(database);
        DB::destroy(&Options::default(), TEST_NAME).unwrap();
    }
}
