use std::{error, fmt};

use base64::Engine;
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use reqwest::{IntoUrl, Url};
use serde::{Serialize, de::DeserializeOwned};

#[derive(Debug)]
pub enum Error {
    Reqwest(reqwest::Error),
    Encode(rmp_serde::encode::Error),
    Decode(rmp_serde::decode::Error),
}

impl From<reqwest::Error> for Error {
    fn from(value: reqwest::Error) -> Self {
        Self::Reqwest(value)
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(value: rmp_serde::encode::Error) -> Self {
        Self::Encode(value)
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(value: rmp_serde::decode::Error) -> Self {
        Self::Decode(value)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Reqwest(error) => error.fmt(f),
            Error::Encode(error) => error.fmt(f),
            Error::Decode(error) => error.fmt(f),
        }
    }
}

impl error::Error for Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct Bespoke {
    client: reqwest::Client,
    url: Url,
}

impl Bespoke {
    pub fn new(url: impl IntoUrl) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::new(),
            url: url.into_url()?,
        })
    }

    pub async fn set<K, V>(&self, key: &K, value: &V) -> Result<()>
    where
        K: ?Sized,
        V: ?Sized,
        K: Serialize,
        V: Serialize,
    {
        let key = rmp_serde::to_vec(key)?;
        let value = rmp_serde::to_vec(value)?;
        self.set_bytes(&key, &value).await
    }

    pub async fn get<K, V>(&self, key: &K) -> Result<Option<V>>
    where
        K: ?Sized,
        K: Serialize,
        V: DeserializeOwned,
    {
        let key = rmp_serde::to_vec(key)?;

        match self.get_bytes(&key).await? {
            Some(bytes) => rmp_serde::from_slice(&bytes).map_err(Into::into),
            None => Ok(None),
        }
    }

    pub async fn subscribe<K, V>(
        &self,
        key: &K,
    ) -> Result<impl Stream<Item = Result<V>> + use<K, V>>
    where
        K: ?Sized,
        K: Serialize,
        V: DeserializeOwned,
    {
        let key = rmp_serde::to_vec(key)?;

        Ok(self
            .subscribe_bytes(&key)
            .await?
            .map(|item| rmp_serde::from_slice(&item?).map_err(Into::into)))
    }

    pub async fn remove<K>(&self, key: &K) -> Result<()>
    where
        K: ?Sized,
        K: Serialize,
    {
        let key = rmp_serde::to_vec(key)?;
        self.remove_bytes(&key).await
    }

    pub async fn set_bytes(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.client
            .post(self.key_url(key))
            .body(value.to_vec())
            .send()
            .await?;

        Ok(())
    }

    pub async fn get_bytes(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let mut response = self.client.get(self.key_url(key)).send().await?;

        response.chunk().await.map_err(Into::into)
    }

    pub async fn subscribe_bytes(
        &self,
        key: &[u8],
    ) -> Result<impl Stream<Item = Result<Bytes>> + use<>> {
        let response = self.client.get(self.key_url(key)).send().await?;

        Ok(response.bytes_stream().map_err(Into::into))
    }

    pub async fn remove_bytes(&self, key: &[u8]) -> Result<()> {
        self.client.delete(self.key_url(key)).send().await?;

        Ok(())
    }

    fn key_url(&self, key: &[u8]) -> Url {
        let key = base64::engine::general_purpose::URL_SAFE.encode(key);
        self.url.join("v1/").unwrap().join(&key).unwrap()
    }
}
