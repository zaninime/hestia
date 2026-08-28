//! Pluggable cache-client seam.
//!
//! Every caller that needs the GHA cache talks to a [`CacheClient`] instead
//! of a concrete backend. Today that is the v2 (Twirp) client; the v1
//! (`_apis/artifactcache`) backend plugs in as a second enum arm.

use bytes::Bytes;

use crate::gha::Error;
use crate::gha::twirp::TwirpClient;

/// Environment variable that opts into the cache v1 (`_apis/artifactcache`)
/// API. Presence alone matters; the value is ignored.
pub const ENV_CACHE_API_V1: &str = "HESTIA_CACHE_API_V1";

/// Cache `version` namespace for the v1 backend: sha256 of "hestia-1".
///
/// Like the v2 namespace ([`crate::gha::twirp::CACHE_VERSION`]) it is a
/// namespace, not a format version: changing it orphans every v1 cache
/// entry.
pub const V1_CACHE_VERSION: &str =
    "7a32118639289175533829e84c9aaa9fa781f6a5f1b18a9c8a6bd3642b39dd88";

/// Result of reserving a cache entry for upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reservation {
    /// Key reserved; upload the blob to this pre-signed Azure URL, then call
    /// [`TwirpClient::finalize_upload`].
    Created { token: String },
    /// The key+version already exists (reserved or finalized). For
    /// content-addressed keys this means the data is already present.
    AlreadyExists,
}

/// Result of looking up a cache entry for download.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadUrl {
    /// Entry found; `matched_key` is the full key (relevant for prefix
    /// restore-key matches).
    Hit { url: String, matched_key: String },
    /// No entry matches.
    Miss,
}

/// One pluggable cache backend. The v2 Twirp client is the only arm today;
/// the v1 client joins it behind [`ENV_CACHE_API_V1`] in a later change.
#[derive(Debug, Clone)]
pub enum CacheClient {
    V2(TwirpClient),
}

impl CacheClient {
    /// Build a client from the process environment. Always selects the v2
    /// backend for now (the v1 dispatch lands in a later change).
    pub fn from_env(http: reqwest::Client) -> Result<CacheClient, Error> {
        TwirpClient::from_env(http).map(CacheClient::V2)
    }

    /// Test-only constructor: a v2 client whose URL is never dereferenced.
    /// Used by unit tests that need a [`CacheClient`] value but exercise no
    /// network path.
    #[cfg(test)]
    pub(crate) fn v2_for_tests(http: reqwest::Client) -> CacheClient {
        CacheClient::V2(TwirpClient::new(http, "http://unused", "token"))
    }

    /// The cache `version` namespace this client writes and reads.
    pub fn version(&self) -> &str {
        match self {
            CacheClient::V2(inner) => inner.version(),
        }
    }

    /// Reserve `key` for upload.
    pub async fn create_cache_entry(&self, key: &str) -> Result<Reservation, Error> {
        match self {
            CacheClient::V2(inner) => inner.create_cache_entry(key).await,
        }
    }

    /// Look up a download URL for `key`, prefix-matching `restore_keys`.
    pub async fn get_download_url(
        &self,
        key: &str,
        restore_keys: &[&str],
    ) -> Result<DownloadUrl, Error> {
        match self {
            CacheClient::V2(inner) => inner.get_download_url(key, restore_keys).await,
        }
    }

    /// Upload `data` to a reserved entry's `token`, then finalize it.
    pub async fn upload_and_finalize(
        &self,
        http: &reqwest::Client,
        key: &str,
        token: String,
        data: Bytes,
    ) -> Result<(), Error> {
        match self {
            CacheClient::V2(inner) => inner.upload_and_finalize(http, key, token, data).await,
        }
    }

    /// Whether the runtime token may write to the cache.
    pub async fn probe_writable(&self) -> Result<bool, Error> {
        match self {
            CacheClient::V2(inner) => inner.probe_writable().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_cache_version_is_sha256_of_hestia_1() {
        let hex = crate::manifest::Hash32::digest(b"hestia-1").to_hex();
        assert_eq!(V1_CACHE_VERSION, hex);
    }
}
