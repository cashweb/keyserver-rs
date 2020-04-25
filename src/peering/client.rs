use std::{collections::HashSet, str::FromStr};

use bytes::Bytes;
use http::{header::AUTHORIZATION, uri::InvalidUri};
use hyper::{
    body::aggregate, client::connect::Connect, Body, Client, Error as HyperError, Request, Uri,
};
use prost::{DecodeError, Message as _};

use crate::{models::keyserver::Peers, METADATA_PATH};

#[derive(Clone)]
pub struct PeeringClient<C>(Client<C, Body>);

#[derive(Debug)]
pub enum PeerError {
    Hyper(HyperError),
    Decode(DecodeError),
    Uri(InvalidUri),
}

impl From<HyperError> for PeerError {
    fn from(err: HyperError) -> Self {
        Self::Hyper(err)
    }
}

impl From<InvalidUri> for PeerError {
    fn from(err: InvalidUri) -> Self {
        Self::Uri(err)
    }
}

impl<C> PeeringClient<C>
where
    C: Clone + Send + Sync,
    C: Connect + 'static,
{
    pub fn new(connector: C) -> Self {
        PeeringClient(Client::builder().build::<_, Body>(connector))
    }

    pub async fn get_peers(&self, url: String) -> Result<Vec<String>, PeerError> {
        let uri = Uri::from_str(&format!("{}/peers", url))?;
        let response = self.0.get(uri).await?;
        let raw = aggregate(response.into_body()).await?;
        let peers = Peers::decode(raw).map_err(PeerError::Decode)?;
        Ok(peers
            .peers
            .into_iter()
            .map(|peer| peer.url)
            .collect())
    }

    pub async fn get_fan(&self, url_set: &HashSet<String>) -> HashSet<String> {
        let fan = url_set.iter().map(|url| self.get_peers(url.clone()));
        let new_urls: HashSet<_> = futures::future::join_all(fan)
            .await
            .into_iter()
            .filter_map(|urls| urls.ok())
            .flatten()
            .collect();
        new_urls
    }

    pub async fn put_metadata(
        &self,
        url: &str,
        addr: &str,
        metadata: Bytes,
        token: &str,
    ) -> Result<(), PeerError> {
        let uri = Uri::from_str(&format!("{}/{}/{}", url, METADATA_PATH, addr))?;
        let request = Request::put(uri)
            .header(AUTHORIZATION, format!("POP {}", token))
            .body(Body::from(metadata))
            .unwrap();
        self.0.request(request).await?;
        Ok(())
    }
}
