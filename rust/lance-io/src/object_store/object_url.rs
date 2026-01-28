// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
use std::sync::Arc;
use std::time::Duration;

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

/// Object URL provider that derives URLs from the store prefix and optional signer.
#[derive(Debug)]
pub struct ObjectUrl {
    store_prefix: String,
    signer: Option<SignerRef>,
    opendal_operator: Option<OperatorRef>,
}

impl ObjectUrl {
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

    /// Generate a URL for an object at the given path.
    ///
    /// Returns `Ok(Some(url))` when a URL can be generated, `Ok(None)` when the
    /// backend does not support signed URLs or when no URL is available, and
    /// `Err` for unexpected errors.
    pub async fn presigned_url(&self, path: &Path, expires: Duration) -> Result<Option<Url>> {
        presigned_url(&self.signer, &self.opendal_operator, path, expires).await
    }

    /// Generate a stable unique URL derived from the store prefix and path.
    pub fn location_uri(&self, path: &Path) -> Result<Url> {
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
