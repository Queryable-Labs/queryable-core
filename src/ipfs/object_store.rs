use std::fmt;
use bytes::Bytes;
use async_trait::async_trait;
use std::fmt::{Debug, Display, Formatter};
use futures::{Stream, stream, StreamExt, TryStreamExt};
use std::ops::Range;
use futures::stream::BoxStream;
use chrono::{Utc, TimeZone};
use log::debug;
use tokio::io::AsyncWrite;
use ipfs_api_backend_hyper::IpfsApi;
use object_store::{Error, GetResult, ListResult, ObjectMeta, ObjectStore, Result, path::Path, MultipartId};
use crate::ipfs::client::IpfsClient;
use crate::types::datasource::PublishedBlockItem;

pub struct IPFSFileSystem {
    pub client: IpfsClient,
    pub block_items: Vec<PublishedBlockItem>,
}

impl Debug for IPFSFileSystem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IPFSFileSystem")
    }
}

impl Display for IPFSFileSystem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IPFSFileSystem")
    }
}

#[derive(Debug)]
struct IPFSStatError;

impl fmt::Display for IPFSStatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Path is not present in block items")
    }
}

impl std::error::Error for IPFSStatError {}

impl IPFSFileSystem {
    pub fn new(
        client: IpfsClient,
        block_items: Vec<PublishedBlockItem>,
    ) -> Self {
        Self {
            client,
            block_items,
        }
    }

    async fn get_object_stat(
        &self,
        location: &Path
    ) -> Result<ObjectMeta> {
        let key = location.as_ref();

        let key_path = format!("/{}", key.clone());

        debug!("Request file stats in cache {}", key_path);

        for block_item in self.block_items.iter() {
            if block_item.ipfs_path.eq(&key_path) {
                return Ok(
                    ObjectMeta {
                        location: location.clone(),
                        last_modified: Utc.timestamp_opt(
                            block_item.created_at.clone() as i64,
                            0
                        ).unwrap(),
                        size: block_item.size as usize,
                    }
                )
            }
        }

        debug!("Request file stats in IPFS {}", key_path);

        Err(
            Error::NotFound {
                path: location.to_string(),
                source: Box::new(IPFSStatError {}),
            }
        )
    }

    async fn get_object(
        &self,
        location: &Path,
        range: Option<Range<usize>>,
    ) -> Result<Bytes> {
        let key = location.as_ref();

        let key_path = format!("/{}", key.clone());

        debug!("Request file in IPFS {} with options {:?}", key_path, range);

        if range.is_some() {
            let unwrapped_range = range.unwrap();

            let offset = unwrapped_range.start;
            let length = unwrapped_range.end - unwrapped_range.start;

            Ok(
                collect_bytes(
                    self.client.cat_range(
                        key_path.as_str(),
                        offset,
                        length.clone()
                    )
                        .map_err(|e| match e {
                            _ => Error::Generic {
                                store: "IPFS",
                                source: e.into(),
                            },
                        }),
                    Some(length)
                ).await?
            )
        } else {
            Ok(
                collect_bytes(
                    self.client.cat(key_path.as_str())
                        .map_err(|e| match e {
                            _ => Error::Generic {
                                store: "IPFS",
                                source: e.into(),
                            },
                        }),
                    Option::None
                ).await?
            )
        }
    }
}

#[async_trait]
impl ObjectStore for IPFSFileSystem {
    /// Save the provided bytes to the specified location.
    async fn put(&self, _location: &Path, _bytes: Bytes) -> Result<()> {
        Err(Error::NotImplemented)
    }

    async fn put_multipart(&self, _: &Path) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        Err(Error::NotImplemented)
    }

    async fn abort_multipart(&self, _: &Path, _: &MultipartId) -> Result<()> {
        Err(Error::NotImplemented)
    }

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &Path) -> Result<GetResult> {
        let bytes = self.get_object(location, Option::None).await?;

        Ok(GetResult::Stream(
            stream::once(async move { Ok(bytes) }).boxed()
        ))
    }

    /// Return the bytes that are stored at the specified location
    /// in the given byte range
    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let bytes = self.get_object(location, Some(range)).await?;

        Ok(bytes)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        let mut result = vec![];

        for range in ranges {
            let bytes = self.get_object(location, Some(range.clone())).await?;

            result.push(bytes);
        }

        Ok(result)
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.get_object_stat(location).await
    }

    /// Delete the object at the specified location.
    async fn delete(&self, _location: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list(&self, _prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        Err(Error::NotImplemented)
    }

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        Err(Error::NotImplemented)
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    async fn rename_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }
}

/// Collect a stream into [`Bytes`] avoiding copying in the event of a single chunk, as-is copy from object_store
async fn collect_bytes<S>(mut stream: S, size_hint: Option<usize>) -> Result<Bytes>
    where
        S: Stream<Item = Result<Bytes>> + Send + Unpin,
{
    let first = stream.next().await.transpose()
        .map_err(|e| match e {
            _ => Error::Generic {
                store: "IPFS",
                source: e.into(),
            },
        })?.unwrap_or_default();

    // Avoid copying if single response
    match stream.next().await.transpose().map_err(|e| match e {
        _ => Error::Generic {
            store: "IPFS",
            source: e.into(),
        },
    })? {
        None => Ok(first),
        Some(second) => {
            let size_hint = size_hint.unwrap_or_else(|| first.len() + second.len());

            let mut buf = Vec::with_capacity(size_hint);
            buf.extend_from_slice(&first);
            buf.extend_from_slice(&second);
            while let Some(maybe_bytes) = stream.next().await {
                buf.extend_from_slice(&maybe_bytes?);
            }

            Ok(buf.into())
        }
    }
}