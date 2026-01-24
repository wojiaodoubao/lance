// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use object_store::path::Path;
#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
use object_store::signer::Signer;
#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
use opendal::Operator;
#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
use reqwest::Method;
use snafu::location;
use url::Url;

use lance_core::{Error, Result};

#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
type SignerRef = Arc<dyn Signer>;

#[cfg(not(any(feature = "aws", feature = "azure", feature = "gcp")))]
type SignerRef = ();

#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
type OperatorRef = Operator;

#[cfg(not(any(feature = "aws", feature = "azure", feature = "gcp")))]
type OperatorRef = ();

/// Trait for generating URLs for objects in an object store. .
#[async_trait]
pub trait ObjectUrl: std::fmt::Debug + Send + Sync {
    /// Generate a URL for an object at the given path.
    ///
    /// Returns `Ok(Some(url))` when a URL can be generated, `Ok(None)` when the
    /// backend does not support signed URLs or when no URL is available, and
    /// `Err` for unexpected errors.
    async fn signed_url(&self, path: &Path, expires: Duration) -> Result<Option<Url>>;

    /// Generate a publicly accessible URL for an object.
    ///
    /// Returns `Ok(Some(url))` when a URL can be generated, `Ok(None)` when the
    /// backend does not support public URLs or when no URL is available, and
    /// `Err` for unexpected errors.
    fn public_url(&self, path: &Path) -> Result<Option<Url>>;

    /// Generate a stable unique object URL derived from the store prefix and path.
    fn object_url(&self, path: &Path) -> Result<Url>;
}

/// Amazon S3 URL provider that derives URLs from the store prefix and optional signer.
#[derive(Debug)]
pub struct S3ObjectUrl {
    store_prefix: String,
    bucket: String,
    endpoint: String,
    signer: Option<SignerRef>,
    opendal_operator: Option<OperatorRef>,
}

impl S3ObjectUrl {
    pub fn new(
        store_prefix: String,
        bucket: String,
        endpoint: String,
        signer: Option<SignerRef>,
        opendal_operator: Option<OperatorRef>,
    ) -> Self {
        Self {
            store_prefix,
            bucket,
            endpoint,
            signer,
            opendal_operator,
        }
    }
}

#[async_trait]
impl ObjectUrl for S3ObjectUrl {
    async fn signed_url(&self, path: &Path, expires: Duration) -> Result<Option<Url>> {
        presigned_url(&self.signer, &self.opendal_operator, path, expires).await
    }

    fn public_url(&self, path: &Path) -> Result<Option<Url>> {
        let mut p = path.to_string();
        while p.starts_with('/') {
            p.remove(0);
        }
        let url_str = format!("https://{}.{}/{}", self.bucket, self.endpoint, p);
        let url = Url::parse(&url_str).map_err(|e| {
            Error::invalid_input(format!("Invalid URL {}: {}", url_str, e), location!())
        })?;
        Ok(Some(url))
    }

    fn object_url(&self, path: &Path) -> Result<Url> {
        object_url_from_store_prefix(&self.store_prefix, path)
    }
}

#[derive(Debug)]
pub struct AzureObjectUrl {
    store_prefix: String,
    signer: Option<SignerRef>,
    opendal_operator: Option<OperatorRef>,
}

#[async_trait]
impl ObjectUrl for AzureObjectUrl {
    async fn signed_url(&self, path: &Path, expires: Duration) -> Result<Option<Url>> {
        presigned_url(&self.signer, &self.opendal_operator, path, expires).await
    }

    fn public_url(&self, _path: &Path) -> Result<Option<Url>> {
        Ok(None)
    }

    fn object_url(&self, path: &Path) -> Result<Url> {
        object_url_from_store_prefix(&self.store_prefix, path)
    }
}

impl AzureObjectUrl {
    pub fn new(
        store_prefix: String,
        signer: Option<SignerRef>,
        opendal_operator: Option<OperatorRef>,
    ) -> Self {
        Self {
            store_prefix,
            signer,
            opendal_operator,
        }
    }
}

#[derive(Debug)]
pub struct GcpObjectUrl {
    store_prefix: String,
    signer: Option<SignerRef>,
    opendal_operator: Option<OperatorRef>,
}

#[async_trait]
impl ObjectUrl for GcpObjectUrl {
    async fn signed_url(&self, path: &Path, expires: Duration) -> Result<Option<Url>> {
        presigned_url(&self.signer, &self.opendal_operator, path, expires).await
    }

    fn public_url(&self, _path: &Path) -> Result<Option<Url>> {
        Ok(None)
    }

    fn object_url(&self, path: &Path) -> Result<Url> {
        object_url_from_store_prefix(&self.store_prefix, path)
    }
}

impl GcpObjectUrl {
    pub fn new(
        store_prefix: String,
        signer: Option<SignerRef>,
        opendal_operator: Option<OperatorRef>,
    ) -> Self {
        Self {
            store_prefix,
            signer,
            opendal_operator,
        }
    }
}

/// URL provider only supports `object_url`.
#[derive(Debug, Default)]
pub struct SimpleObjectUrl {
    store_prefix: String,
}

impl SimpleObjectUrl {
    pub fn new(store_prefix: String) -> Self {
        Self { store_prefix }
    }
}

#[async_trait]
impl ObjectUrl for SimpleObjectUrl {
    async fn signed_url(&self, _path: &Path, _expires: Duration) -> Result<Option<Url>> {
        Ok(None)
    }

    fn public_url(&self, _path: &Path) -> Result<Option<Url>> {
        Ok(None)
    }

    fn object_url(&self, path: &Path) -> Result<Url> {
        object_url_from_store_prefix(&self.store_prefix, path)
    }
}

async fn presigned_url(
    signer: &Option<SignerRef>,
    opendal_operator: &Option<OperatorRef>,
    path: &Path,
    expires: Duration,
) -> Result<Option<Url>> {
    #[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
    {
        if let Some(signer) = signer.as_ref() {
            let url = signer
                .signed_url(Method::GET, path, expires)
                .await
                .map_err(Error::from)?;
            return Ok(Some(url));
        }
        if let Some(operator) = opendal_operator.as_ref() {
            let request = operator
                .presign_read(path.as_ref(), expires)
                .await
                .map_err(|e| {
                    Error::invalid_input(
                        format!("Failed to presign read request: {}", e),
                        location!(),
                    )
                })?;
            let uri = request.uri().to_string();
            let url = Url::parse(&uri).map_err(|e| {
                Error::invalid_input(format!("Invalid presigned URL {}: {}", uri, e), location!())
            })?;
            return Ok(Some(url));
        }
        Ok(None)
    }

    #[cfg(not(any(feature = "aws", feature = "azure", feature = "gcp")))]
    {
        let _ = (path, expires, signer, opendal_operator);
        Ok(None)
    }
}

/// Build a URL from a store prefix and relative object path.
fn object_url_from_store_prefix(store_prefix: &str, path: &Path) -> Result<Url> {
    let (scheme, authority) = match store_prefix.split_once('$') {
        Some((scheme, authority)) => (scheme, authority),
        None => (store_prefix, ""),
    };

    let mut p = path.to_string();
    while p.starts_with('/') {
        p.remove(0);
    }

    let full = if authority.is_empty() {
        format!("{scheme}:///{p}")
    } else {
        format!("{scheme}://{authority}/{p}")
    };

    Url::parse(&full).map_err(|e| {
        Error::invalid_input(
            format!(
                "Invalid URL derived from store_prefix '{}' and path '{}': {}",
                store_prefix, path, e
            ),
            location!(),
        )
    })
}
