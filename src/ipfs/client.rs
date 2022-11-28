use std::fs::File;
use std::path::PathBuf;
use async_trait::async_trait;
use futures::{TryStreamExt};
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use ipfs_api_prelude::Backend;
use ipfs_api_backend_hyper::IpfsClient as BackendIpfsClient;
pub use ipfs_api_backend_hyper::IpfsApi;
pub use ipfs_api_backend_hyper::TryFromUri;
use ipfs_api_backend_hyper::response::{AddResponse, NamePublishResponse};
use log::{debug, trace};
use crate::ipfs::name_publish::NamePublish;
use crate::types::error::IpfsError;

pub type IpfsClient = BackendIpfsClient<HttpsConnector<HttpConnector>>;

/// Instantiating new IpfsClient as it does not implement Clone trait
pub fn get_ipfs_client(ipfs_url: &str) -> Result<IpfsClient, IpfsError> {
    Ok(
        IpfsClient::from_str(ipfs_url)?
    )
}

#[async_trait]
pub trait CatOps {
    async fn cat_str(&self, path: String) -> Result<String, IpfsError>;
    async fn cat_vec(&self, path: String) -> Result<Vec<u8>, IpfsError>;
}

#[async_trait]
impl CatOps for IpfsClient {
    async fn cat_str(&self, path: String) -> Result<String, IpfsError> {
        let vec = self.cat(path.as_str())
            .map_ok(|chunk| chunk.to_vec())
            .try_concat()
            .await?;

        Ok(
            String::from_utf8(vec)
                .map_err(IpfsError::ParseError)?
        )
    }

    async fn cat_vec(&self, path: String) -> Result<Vec<u8>, IpfsError> {
        Ok(
            self.cat(path.as_str())
            .map_ok(|chunk| chunk.to_vec())
            .try_concat()
            .await?
        )
    }
}

#[async_trait]
pub trait AddLocalOps {
    async fn add_local(&self, path: PathBuf) -> Result<AddResponse, IpfsError>;
}

#[async_trait]
impl AddLocalOps for IpfsClient {
    async fn add_local(&self, path: PathBuf) -> Result<AddResponse, IpfsError> {
        debug!("Uploading file {}", path.to_str().unwrap().to_string());

        let file = File::open(path)?;

        let result = self.add(file).await;

        trace!("Uploaded file");

        match result {
            Ok(file) => {
                return Ok(file);
            },
            Err(e) => {
                return Err(e.into());
            },
        }
    }
}

#[async_trait]
pub trait NamePublishOps {
    async fn name_publish_v2(&self, path: String, ipns_key: String) -> Result<NamePublishResponse, IpfsError>;
}

#[async_trait]
impl NamePublishOps for IpfsClient {
    async fn name_publish_v2(&self, hash: String, key: String) -> Result<NamePublishResponse, IpfsError> {
        let result = self.request(
            NamePublish {
                path: format!("/ipfs/{}", hash).as_str(),
                resolve: true,
                offline: true,
                lifetime: Some("8760h"),
                ttl: Some("1m"),
                key: Some(key.as_str()),
            },
            None,
        ).await;

        match result {
            Ok(name_publish) => {
                return Ok(name_publish);
            },
            Err(err) => {
                return Err(err.into());
            }
        }
    }
}