use std::{collections::VecDeque, sync::Arc};

use bitcoincash_addr::Address;
use bytes::Bytes;
use dashmap::DashMap;
use hyper::client::connect::Connect;
use rand::{rngs::OsRng, seq::IteratorRandom};
use tokio::sync::RwLock;

use super::PeerHandler;
use crate::{db::Database, SETTINGS};

#[derive(Clone)]
pub struct TokenCache {
    tokens_blocks: Arc<RwLock<VecDeque<DashMap<Address, String>>>>,
}

impl TokenCache {
    pub fn new() -> Self {
        let deque = VecDeque::from(vec![Default::default(); SETTINGS.peering.broadcast_delay]);
        Self {
            tokens_blocks: Arc::new(RwLock::new(deque)),
        }
    }

    pub async fn add_token(&self, addr: Address, token: String) {
        let token_blocks = self.tokens_blocks.read().await;
        // TODO: Check previous blocks?
        // TODO: Check consistency garauntees of the dashmap under iter + insert
        token_blocks.front().unwrap().insert(addr, token); // TODO: Double check this is safe
    }

    pub async fn broadcast_block<C>(&self, peer_state: &PeerHandler<C>, db: &Database)
    where
        C: Clone + Send + Sync,
        C: Connect + 'static,
    {
        let mut token_blocks = self.tokens_blocks.write().await;

        // Cycle blocks
        token_blocks.push_front(Default::default());
        let token_block = match token_blocks.pop_back() {
            Some(some) => some,
            None => return,
        };

        // Unpack peer state
        let peers = peer_state.peers.read().await;
        let client = peer_state.client.clone();

        // Sample peers
        let url_choices: Vec<_> = peers
            .iter()
            .choose_multiple(&mut OsRng, SETTINGS.peering.fan_size);

        for (addr, token) in token_block.into_iter() {
            let metadata = match db.get_raw_metadata(addr.as_body()) {
                Ok(Some(some)) => some,
                _ => continue,
            };
            let addr_str = addr.encode().unwrap(); // This is safe
            let metadata = Bytes::from(metadata);
            for url in &url_choices {
                // TODO: Make this non-blocking
                log::info!("{}", url);

                if let Err(err) = client
                    .put_metadata(&url, &addr_str, metadata.clone(), &token)
                    .await
                {
                    log::error!("{:?}", err);
                    // TODO: Error handling -> remove as peer
                }
            }
        }
    }
}
