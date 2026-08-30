//! Cache v1 (`_apis/artifactcache`) client for Gitea / Forgejo.
//!
//! Gitea and Forgejo run a cache service whose wire protocol is the
//! `_apis/artifactcache` REST surface that `actions/cache` speaks, rather
//! than GitHub's Twirp service. Endpoint layout, ported from
//! `tonistiigi/go-actions-cache` (`cache.go` v1 paths) and
//! `actions/toolkit` (`cacheHttpClient.ts`):
//!
//! ```text
//! POST  {ACTIONS_CACHE_URL}_apis/artifactcache/caches          reserve
//! PATCH {ACTIONS_CACHE_URL}_apis/artifactcache/caches/{id}     chunk upload
//! POST  {ACTIONS_CACHE_URL}_apis/artifactcache/caches/{id}     commit
//! GET   {ACTIONS_CACHE_URL}_apis/artifactcache/cache?keys=..&version=..
//! ```
//!
//! Every request carries `Authorization: Bearer {ACTIONS_RUNTIME_TOKEN}`.
//!
//! Where the v1 service differs from v2 ([`crate::gha::twirp`]):
//!
//! * Entry already exists: reserve answers HTTP **409** — the status alone
//!   decides, the body is ignored.
//! * Cache miss: lookup answers HTTP **204**, or a 200 whose body is empty
//!   or has no `cacheKey`.
//! * Read-only token: reserve answers HTTP **403**.
//! * The `version` namespace is always [`V1_CACHE_VERSION`] (sha256 of
//!   "hestia-1"). Unlike the v2 client, this client ignores
//!   `HESTIA_CACHE_VERSION_SALT`; the namespace cannot be salted.

use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::gha::client::{DownloadUrl, Reservation, V1_CACHE_VERSION};
use crate::gha::twirp::ENV_RUNTIME_TOKEN;
use crate::gha::{Error, blob};

/// `ACTIONS_CACHE_URL` points at the forge's artifact cache mount, e.g.
/// `https://gitea.example/api/actions/cache/`. Gitea and Forgejo set it on
/// their runners; GitHub does not (it only sets `ACTIONS_RESULTS_URL`).
pub const ENV_CACHE_URL: &str = "ACTIONS_CACHE_URL";

/// Size of one PATCH chunk: `actions/toolkit` uses 32 MiB on 64-bit hosts.
const UPLOAD_CHUNK_SIZE: usize = 32 * 1024 * 1024;

/// Transient-failure retry budget per chunk, mirroring
/// [`crate::gha::blob::put_with_refresh`].
const UPLOAD_RETRIES: u32 = 3;

/// First retry delay; doubles per attempt.
const UPLOAD_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Key [`V1Client::probe_writable`] reserves to test write access. Fixed so
/// a writable token can reserve it at most once; it is never finalized and
/// the unfinished reservation expires.
const WRITE_PROBE_KEY: &str = "hestia-write-probe-v1";

/// `POST /caches` body. Serialized with the field names the service expects
/// (`key`, `version`).
#[derive(Debug, Serialize)]
struct ReserveCacheRequest {
    key: String,
    version: String,
}

/// `POST /caches` 200 body: the id later used in `caches/{id}`.
#[derive(Debug, Deserialize)]
struct ReserveCacheResponse {
    #[serde(rename = "cacheId")]
    cache_id: u64,
}

/// `POST /caches/{id}` body.
#[derive(Debug, Serialize)]
struct CommitCacheRequest {
    size: u64,
}

/// `GET /cache` 200 body.
#[derive(Debug, Default, Deserialize)]
struct CacheEntryResponse {
    #[serde(default, rename = "cacheKey")]
    cache_key: String,
    #[serde(default, rename = "archiveLocation")]
    archive_location: String,
}

/// v1 cache client: Gitea / Forgejo's `_apis/artifactcache` API.
#[derive(Debug, Clone)]
pub struct V1Client {
    http: reqwest::Client,
    base_url: String,
    token: String,
    version: String,
}

impl V1Client {
    /// Build a client for `cache_url` (`ACTIONS_CACHE_URL`, the forge's
    /// cache mount) authenticated with `runtime_token`
    /// (`ACTIONS_RUNTIME_TOKEN`).
    pub fn new(
        http: reqwest::Client,
        cache_url: impl Into<String>,
        runtime_token: impl Into<String>,
    ) -> Self {
        Self {
            http,
            base_url: cache_url.into(),
            token: runtime_token.into(),
            version: V1_CACHE_VERSION.to_string(),
        }
    }

    /// Build a client from `ACTIONS_CACHE_URL` / `ACTIONS_RUNTIME_TOKEN`.
    ///
    /// An empty value counts as missing ([`Error::MissingEnv`]). Unlike the
    /// v2 client this one ignores `HESTIA_CACHE_VERSION_SALT`: the `version`
    /// namespace is fixed at [`V1_CACHE_VERSION`].
    pub fn from_env(http: reqwest::Client) -> Result<Self, Error> {
        let url = std::env::var(ENV_CACHE_URL).map_err(|_| Error::MissingEnv(ENV_CACHE_URL))?;
        let token =
            std::env::var(ENV_RUNTIME_TOKEN).map_err(|_| Error::MissingEnv(ENV_RUNTIME_TOKEN))?;
        if url.is_empty() {
            return Err(Error::MissingEnv(ENV_CACHE_URL));
        }
        if token.is_empty() {
            return Err(Error::MissingEnv(ENV_RUNTIME_TOKEN));
        }
        Ok(Self::new(http, url, token))
    }

    /// The cache `version` namespace this client writes and reads.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Full URL of one `_apis/artifactcache` resource.
    fn api_url(&self, resource: &str) -> String {
        format!(
            "{}/_apis/artifactcache/{resource}",
            self.base_url.trim_end_matches('/')
        )
    }

    /// `GET /cache` URL: the keys (exact key first, then the distinct
    /// restore keys) comma-joined in `keys`, the namespace in `version`.
    fn lookup_url(&self, key: &str, restore_keys: &[&str]) -> Result<String, Error> {
        let keys = std::iter::once(key)
            .chain(
                restore_keys
                    .iter()
                    .copied()
                    .filter(|&restore| restore != key),
            )
            .collect::<Vec<_>>()
            .join(",");
        let mut url =
            reqwest::Url::parse(&self.api_url("cache")).map_err(|err| Error::InvalidEnv {
                name: ENV_CACHE_URL,
                reason: err.to_string(),
            })?;
        url.query_pairs_mut()
            .append_pair("keys", &keys)
            .append_pair("version", &self.version);
        Ok(url.to_string())
    }

    /// Reserve `key` for upload (`POST /caches`).
    pub async fn create_cache_entry(&self, key: &str) -> Result<Reservation, Error> {
        let request = ReserveCacheRequest {
            key: key.to_string(),
            version: self.version.clone(),
        };
        let url = self.api_url("caches");
        let response = self
            .http
            .post(&url)
            .bearer_auth(&self.token)
            .json(&request)
            .send()
            .await?;
        let status = response.status();
        // Already-exists is decided by the status alone; the body is
        // service-specific noise and must not be inspected.
        if status == reqwest::StatusCode::CONFLICT {
            return Ok(Reservation::AlreadyExists);
        }
        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(Error::TokenExpired {
                method: "reserve".to_string(),
            });
        }
        // Read-only token: distinct from already-exists so that
        // `probe_writable` can tell the two apart.
        if status == reqwest::StatusCode::FORBIDDEN {
            let body = response.text().await.unwrap_or_default();
            let reason = if body.is_empty() {
                "HTTP 403".to_string()
            } else {
                body
            };
            return Err(Error::WriteDenied { reason });
        }
        if !status.is_success() {
            return Err(status_error(&url, response).await);
        }
        let reserved: ReserveCacheResponse = response.json().await?;
        Ok(Reservation::Created {
            token: reserved.cache_id.to_string(),
        })
    }

    /// Whether the runtime token may write to the cache, probed by
    /// reserving a fixed, never-finalized key. Writability is not
    /// advertised, so the only way to know is to attempt a reservation:
    /// writable tokens answer 200 or 409, read-only tokens answer 403.
    pub async fn probe_writable(&self) -> Result<bool, Error> {
        match self.create_cache_entry(WRITE_PROBE_KEY).await {
            Ok(_) => Ok(true),
            Err(Error::WriteDenied { .. }) => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Look up a download URL (`GET /cache`).
    ///
    /// The exact key is always sent first; `restore_keys` follow and are
    /// prefix-matched in order by the service. Misses come as HTTP 204, or
    /// as a 200 with an empty body or no `cacheKey`.
    pub async fn get_download_url(
        &self,
        key: &str,
        restore_keys: &[&str],
    ) -> Result<DownloadUrl, Error> {
        let url = self.lookup_url(key, restore_keys)?;
        let response = self.http.get(&url).bearer_auth(&self.token).send().await?;
        let status = response.status();
        if status == reqwest::StatusCode::NO_CONTENT {
            return Ok(DownloadUrl::Miss);
        }
        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(Error::TokenExpired {
                method: "get_cache_entry".to_string(),
            });
        }
        if !status.is_success() {
            return Err(status_error(&url, response).await);
        }
        let body = response.bytes().await?;
        // Some services answer "nothing here" as a 200 with an empty body.
        if body.is_empty() {
            return Ok(DownloadUrl::Miss);
        }
        let entry: CacheEntryResponse = serde_json::from_slice(&body).map_err(|err| {
            Error::InvalidResponse(format!(
                "cache lookup for {key} returned a 200 whose body does not parse: {err}"
            ))
        })?;
        // A 200 without a cacheKey means no entry matched.
        if entry.cache_key.is_empty() {
            return Ok(DownloadUrl::Miss);
        }
        // A matched entry that points nowhere is a protocol violation.
        if entry.archive_location.is_empty() {
            return Err(Error::InvalidResponse(format!(
                "cache lookup for {key} matched {} without an archiveLocation",
                entry.cache_key
            )));
        }
        Ok(DownloadUrl::Hit {
            url: entry.archive_location,
            matched_key: entry.cache_key,
        })
    }

    pub async fn upload_and_finalize(
        &self,
        http: &reqwest::Client,
        _key: &str,
        token: String,
        data: Bytes,
    ) -> Result<(), Error> {
        let url = self.api_url(&format!("caches/{token}"));
        for (index, chunk) in data.chunks(UPLOAD_CHUNK_SIZE).enumerate() {
            let start = (index * UPLOAD_CHUNK_SIZE) as u64;
            // `end` is inclusive: the last byte index of this chunk.
            let end = start + chunk.len() as u64 - 1;
            let content_range = format!("bytes {start}-{end}/*");
            self.patch_chunk(http, &url, Bytes::copy_from_slice(chunk), &content_range)
                .await?;
        }
        self.commit(&url, data.len() as u64).await
    }

    async fn patch_chunk(
        &self,
        http: &reqwest::Client,
        url: &str,
        chunk: Bytes,
        content_range: &str,
    ) -> Result<(), Error> {
        let mut attempt = 0;
        let mut delay = UPLOAD_RETRY_DELAY;
        loop {
            attempt += 1;
            let response = http
                .patch(url)
                .bearer_auth(&self.token)
                .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                .header(reqwest::header::CONTENT_RANGE, content_range)
                .body(chunk.clone())
                .send()
                .await?;
            let status = response.status();
            if status.is_success() {
                return Ok(());
            }
            let err = status_error(url, response).await;
            if blob::is_transient(&err) && attempt <= UPLOAD_RETRIES {
                tokio::time::sleep(delay).await;
                delay *= 2;
                continue;
            }
            return Err(err);
        }
    }

    /// Commit an uploaded entry (`POST /caches/{id}` with `{"size": ..}`).
    async fn commit(&self, url: &str, size: u64) -> Result<(), Error> {
        let request = CommitCacheRequest { size };
        let response = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .json(&request)
            .send()
            .await?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(Error::TokenExpired {
                method: "commit".to_string(),
            });
        }
        Err(status_error(url, response).await)
    }
}

/// Convert a non-2xx response into [`Error::Status`] with the body text.
async fn status_error(url: &str, response: reqwest::Response) -> Error {
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    Error::Status {
        status,
        url: url.to_string(),
        body,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::body::Bytes as AxumBytes;
    use axum::extract::{DefaultBodyLimit, OriginalUri, Path, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::{get, patch, post};

    fn test_client() -> V1Client {
        V1Client::new(
            reqwest::Client::new(),
            "https://cache.example.com/api/actions/cache/",
            "test-runtime-token",
        )
    }

    /// Fixed stubbed behaviors for the v1 endpoints under test.
    #[derive(Clone)]
    struct StubConfig {
        reserve_status: StatusCode,
        reserve_body: String,
        lookup_status: StatusCode,
        lookup_body: String,
    }

    fn stub_config(
        reserve_status: StatusCode,
        reserve_body: &str,
        lookup_status: StatusCode,
        lookup_body: &str,
    ) -> StubConfig {
        StubConfig {
            reserve_status,
            reserve_body: reserve_body.to_string(),
            lookup_status,
            lookup_body: lookup_body.to_string(),
        }
    }

    /// One HTTP request the stub saw, with the fields under test.
    #[derive(Clone, Default)]
    struct RecordedRequest {
        method: String,
        uri: String,
        content_type: Option<String>,
        content_range: Option<String>,
        authorization: Option<String>,
        body: Vec<u8>,
    }

    #[derive(Clone)]
    struct StubState {
        config: StubConfig,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
    }

    struct Stub {
        base_url: String,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        _task: tokio::task::JoinHandle<()>,
    }

    impl Stub {
        fn client(&self) -> V1Client {
            V1Client::new(
                reqwest::Client::new(),
                self.base_url.clone(),
                "test-runtime-token",
            )
        }

        fn requests(&self) -> Vec<RecordedRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    fn recorded_header(headers: &HeaderMap, name: &str) -> Option<String> {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    }

    async fn reserve_handler(
        State(state): State<StubState>,
        headers: HeaderMap,
        body: AxumBytes,
    ) -> Response {
        state.requests.lock().unwrap().push(RecordedRequest {
            method: "POST".to_string(),
            uri: "/_apis/artifactcache/caches".to_string(),
            content_type: recorded_header(&headers, "content-type"),
            authorization: recorded_header(&headers, "authorization"),
            body: body.to_vec(),
            ..Default::default()
        });
        (
            state.config.reserve_status,
            state.config.reserve_body.clone(),
        )
            .into_response()
    }

    async fn upload_handler(
        State(state): State<StubState>,
        Path(id): Path<String>,
        headers: HeaderMap,
        // Consumed so hyper does not reset a reused connection over the
        // unread 32 MiB chunk body.
        _body: AxumBytes,
    ) -> Response {
        state.requests.lock().unwrap().push(RecordedRequest {
            method: "PATCH".to_string(),
            uri: format!("caches/{id}"),
            content_type: recorded_header(&headers, "content-type"),
            content_range: recorded_header(&headers, "content-range"),
            authorization: recorded_header(&headers, "authorization"),
            ..Default::default()
        });
        StatusCode::OK.into_response()
    }

    async fn commit_handler(
        State(state): State<StubState>,
        Path(id): Path<String>,
        headers: HeaderMap,
        body: AxumBytes,
    ) -> Response {
        state.requests.lock().unwrap().push(RecordedRequest {
            method: "POST".to_string(),
            uri: format!("caches/{id}"),
            content_type: recorded_header(&headers, "content-type"),
            authorization: recorded_header(&headers, "authorization"),
            body: body.to_vec(),
            ..Default::default()
        });
        StatusCode::NO_CONTENT.into_response()
    }

    async fn lookup_handler(
        State(state): State<StubState>,
        OriginalUri(uri): OriginalUri,
    ) -> Response {
        state.requests.lock().unwrap().push(RecordedRequest {
            method: "GET".to_string(),
            uri: uri.to_string(),
            ..Default::default()
        });
        (state.config.lookup_status, state.config.lookup_body.clone()).into_response()
    }

    async fn spawn_stub(config: StubConfig) -> Stub {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = StubState {
            config,
            requests: Arc::clone(&requests),
        };
        let router = Router::new()
            .route("/_apis/artifactcache/caches", post(reserve_handler))
            .route(
                "/_apis/artifactcache/caches/{id}",
                patch(upload_handler)
                    .layer(DefaultBodyLimit::disable())
                    .post(commit_handler),
            )
            .route("/_apis/artifactcache/cache", get(lookup_handler))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub listener");
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Stub {
            base_url,
            requests,
            _task: task,
        }
    }

    fn reserve_ok(reserve_body: &str) -> StubConfig {
        stub_config(StatusCode::OK, reserve_body, StatusCode::NO_CONTENT, "")
    }

    // (a) URL layout ------------------------------------------------------------------

    #[test]
    fn api_url_layout_matches_actions_toolkit() {
        assert_eq!(
            test_client().api_url("caches"),
            "https://cache.example.com/api/actions/cache/_apis/artifactcache/caches"
        );
        assert_eq!(
            test_client().api_url("caches/42"),
            "https://cache.example.com/api/actions/cache/_apis/artifactcache/caches/42"
        );
        // A base URL without a trailing slash still forms one URL.
        let bare = V1Client::new(
            reqwest::Client::new(),
            "https://cache.example.com/api/actions/cache",
            "token",
        );
        assert_eq!(
            bare.api_url("caches"),
            "https://cache.example.com/api/actions/cache/_apis/artifactcache/caches"
        );
    }

    #[test]
    fn lookup_url_joins_keys_and_appends_version() {
        let base = "https://cache.example.com/api/actions/cache/_apis/artifactcache/cache";
        let url = test_client().lookup_url("m#1", &["m#", "unused"]).unwrap();
        assert_eq!(
            url,
            format!("{base}?keys=m%231%2Cm%23%2Cunused&version={V1_CACHE_VERSION}")
        );
        // The exact key is always first; a restore key equal to it is dropped.
        let url = test_client().lookup_url("m#1", &["m#1", "m#"]).unwrap();
        assert_eq!(
            url,
            format!("{base}?keys=m%231%2Cm%23&version={V1_CACHE_VERSION}")
        );
    }

    // (b) Wire serde ------------------------------------------------------------------

    #[test]
    fn request_shapes_serialize_with_snake_case_fields() {
        let reserve = ReserveCacheRequest {
            key: "m#1".into(),
            version: V1_CACHE_VERSION.into(),
        };
        let json = serde_json::to_value(&reserve).unwrap();
        assert_eq!(json["key"], "m#1");
        assert_eq!(json["version"], V1_CACHE_VERSION);

        let commit = CommitCacheRequest { size: 42 };
        let json = serde_json::to_value(&commit).unwrap();
        assert_eq!(json["size"], 42);
    }

    #[test]
    fn response_shapes_deserialize_from_service_json() {
        let reserve: ReserveCacheResponse = serde_json::from_str(r#"{"cacheId": 7}"#).unwrap();
        assert_eq!(reserve.cache_id, 7);

        let entry: CacheEntryResponse = serde_json::from_str(
            r#"{"cacheKey": "m#7", "archiveLocation": "https://blob/x", "future_field": 1}"#,
        )
        .unwrap();
        assert_eq!(entry.cache_key, "m#7");
        assert_eq!(entry.archive_location, "https://blob/x");

        // Missing fields default to empty (miss detection relies on it).
        let empty: CacheEntryResponse = serde_json::from_str("{}").unwrap();
        assert!(empty.cache_key.is_empty());
        assert!(empty.archive_location.is_empty());
    }

    #[test]
    fn reserve_response_parses_the_real_gitea_wire_shape() {
        // Gitea/Forgejo produce `{"cacheId": <rand.Uint64()>}`: unsigned 64-bit id.
        let response: ReserveCacheResponse =
            serde_json::from_str(&format!(r#"{{"cacheId": {}}}"#, u64::MAX)).unwrap();
        assert_eq!(response.cache_id, u64::MAX);
    }

    // (c) Reserve status mapping ------------------------------------------------------

    #[tokio::test]
    async fn reserve_maps_200_cache_id_to_created_token() {
        let stub = spawn_stub(reserve_ok(r#"{"cacheId": 7}"#)).await;
        let reservation = stub.client().create_cache_entry("pack-abc").await.unwrap();
        assert_eq!(
            reservation,
            Reservation::Created {
                token: "7".to_string()
            }
        );

        let reserve = stub
            .requests()
            .into_iter()
            .find(|request| request.uri == "/_apis/artifactcache/caches")
            .expect("reserve request");
        let body: serde_json::Value = serde_json::from_slice(&reserve.body).unwrap();
        assert_eq!(body["key"], "pack-abc");
        assert_eq!(body["version"], V1_CACHE_VERSION);
    }

    #[tokio::test]
    async fn reserve_maps_bare_409_to_already_exists() {
        // The 409 status decides alone: the body is ignored even when it is
        // empty or some service-specific JSON noise.
        for body in [
            "",
            r#"{"typeKey":"ArtifactCacheItemAlreadyExistsException","message":"exists"}"#,
        ] {
            let stub = spawn_stub(stub_config(
                StatusCode::CONFLICT,
                body,
                StatusCode::NO_CONTENT,
                "",
            ))
            .await;
            let reservation = stub.client().create_cache_entry("pack-abc").await.unwrap();
            assert_eq!(reservation, Reservation::AlreadyExists);
        }
    }

    // (d) Lookup status mapping -------------------------------------------------------

    #[tokio::test]
    async fn lookup_maps_204_and_empty_bodies_to_miss() {
        // 204 -> Miss
        let stub = spawn_stub(reserve_ok(r#"{"cacheId": 1}"#)).await;
        assert_eq!(
            stub.client()
                .get_download_url("m#1", &["m#"])
                .await
                .unwrap(),
            DownloadUrl::Miss
        );
        // 200 with an empty body -> Miss
        let stub = spawn_stub(stub_config(
            StatusCode::OK,
            r#"{"cacheId": 1}"#,
            StatusCode::OK,
            "",
        ))
        .await;
        assert_eq!(
            stub.client()
                .get_download_url("m#1", &["m#"])
                .await
                .unwrap(),
            DownloadUrl::Miss
        );
        // 200 without a cacheKey -> Miss
        let stub = spawn_stub(stub_config(
            StatusCode::OK,
            r#"{"cacheId": 1}"#,
            StatusCode::OK,
            r#"{"archiveLocation": "https://blob/x"}"#,
        ))
        .await;
        assert_eq!(
            stub.client()
                .get_download_url("m#1", &["m#"])
                .await
                .unwrap(),
            DownloadUrl::Miss
        );
    }

    #[tokio::test]
    async fn lookup_maps_200_missing_archive_location_to_invalid_response() {
        let stub = spawn_stub(stub_config(
            StatusCode::OK,
            r#"{"cacheId": 1}"#,
            StatusCode::OK,
            r#"{"cacheKey": "m#1"}"#,
        ))
        .await;
        let err = stub
            .client()
            .get_download_url("m#1", &["m#"])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidResponse(_)));
    }

    #[tokio::test]
    async fn lookup_maps_200_full_body_to_hit_and_builds_the_query_uri() {
        let stub = spawn_stub(stub_config(
            StatusCode::OK,
            r#"{"cacheId": 1}"#,
            StatusCode::OK,
            r#"{"cacheKey": "m#7", "archiveLocation": "https://blob/x"}"#,
        ))
        .await;
        let result = stub
            .client()
            .get_download_url("m#1", &["m#"])
            .await
            .unwrap();
        assert_eq!(
            result,
            DownloadUrl::Hit {
                url: "https://blob/x".into(),
                matched_key: "m#7".into(),
            }
        );
        let get = stub
            .requests()
            .into_iter()
            .find(|request| request.method == "GET")
            .expect("lookup request");
        assert_eq!(
            get.uri,
            format!("/_apis/artifactcache/cache?keys=m%231%2Cm%23&version={V1_CACHE_VERSION}")
        );
    }

    // (e) Chunked upload --------------------------------------------------------------

    #[tokio::test]
    async fn upload_patches_one_chunk_then_commits_the_size() {
        let stub = spawn_stub(reserve_ok(r#"{"cacheId": 7}"#)).await;
        let data = Bytes::from(vec![0xCDu8; 5]);
        stub.client()
            .upload_and_finalize(&reqwest::Client::new(), "pack-abc", "7".to_string(), data)
            .await
            .unwrap();

        let requests = stub.requests();
        let patches: Vec<&RecordedRequest> = requests
            .iter()
            .filter(|request| request.method == "PATCH")
            .collect();
        assert_eq!(patches.len(), 1);
        assert_eq!(patches[0].content_range, Some("bytes 0-4/*".to_string()));

        let commit = requests
            .iter()
            .find(|request| request.method == "POST" && request.uri == "caches/7")
            .expect("commit request");
        let body: serde_json::Value = serde_json::from_slice(&commit.body).unwrap();
        assert_eq!(body["size"], 5);
    }

    #[tokio::test]
    async fn upload_splits_data_into_32_mib_patches_with_auth_and_ranges() {
        let stub = spawn_stub(reserve_ok(r#"{"cacheId": 7}"#)).await;
        let total = UPLOAD_CHUNK_SIZE + 3;
        let data = Bytes::from(vec![0xABu8; total]);
        stub.client()
            .upload_and_finalize(&reqwest::Client::new(), "pack-abc", "7".to_string(), data)
            .await
            .unwrap();

        let requests = stub.requests();
        let patches: Vec<&RecordedRequest> = requests
            .iter()
            .filter(|request| request.method == "PATCH")
            .collect();
        assert_eq!(patches.len(), 2);
        assert_eq!(
            patches[0].content_range,
            Some(format!("bytes 0-{}/*", UPLOAD_CHUNK_SIZE - 1))
        );
        assert_eq!(
            patches[1].content_range,
            Some(format!(
                "bytes {UPLOAD_CHUNK_SIZE}-{total}/*",
                total = total - 1
            ))
        );
        for patch in &patches {
            assert_eq!(
                patch.content_type,
                Some("application/octet-stream".to_string())
            );
            assert_eq!(
                patch.authorization,
                Some("Bearer test-runtime-token".to_string())
            );
            assert_eq!(patch.uri, "caches/7");
        }

        // The upload is PATCH + POST against the cache service: no Azure blob
        // PUT anywhere (that would show up as a PUT with x-ms-blob-type).
        assert!(!requests.iter().any(|request| request.method == "PUT"));
        let commit = requests
            .iter()
            .find(|request| request.method == "POST" && request.uri == "caches/7")
            .expect("commit request");
        let body: serde_json::Value = serde_json::from_slice(&commit.body).unwrap();
        assert_eq!(body["size"], total as u64);
        assert_eq!(commit.content_type, Some("application/json".to_string()));
    }

    // (f) Write probe status table ----------------------------------------------------

    #[tokio::test]
    async fn probe_writable_reserves_the_fixed_probe_key() {
        let stub = spawn_stub(reserve_ok(r#"{"cacheId": 1}"#)).await;
        assert!(stub.client().probe_writable().await.unwrap());
        let reserve = stub
            .requests()
            .into_iter()
            .find(|request| request.uri == "/_apis/artifactcache/caches")
            .expect("reserve request");
        let body: serde_json::Value = serde_json::from_slice(&reserve.body).unwrap();
        assert_eq!(body["key"], WRITE_PROBE_KEY);
    }

    #[tokio::test]
    async fn probe_writable_status_table() {
        // 200 / 409 both mean the token may write (the 409: an earlier probe
        // already reserved the fixed key).
        for status in [StatusCode::OK, StatusCode::CONFLICT] {
            let stub = spawn_stub(stub_config(
                status,
                r#"{"cacheId": 1}"#,
                StatusCode::NO_CONTENT,
                "",
            ))
            .await;
            assert!(stub.client().probe_writable().await.unwrap(), "{status}");
        }
        // 403: read-only token.
        let stub = spawn_stub(stub_config(
            StatusCode::FORBIDDEN,
            "forbidden",
            StatusCode::NO_CONTENT,
            "",
        ))
        .await;
        assert!(!stub.client().probe_writable().await.unwrap());
        // 401: rejected token.
        let stub = spawn_stub(stub_config(
            StatusCode::UNAUTHORIZED,
            "",
            StatusCode::NO_CONTENT,
            "",
        ))
        .await;
        let err = stub.client().probe_writable().await.unwrap_err();
        assert!(matches!(err, Error::TokenExpired { method } if method == "reserve"));
        // 429 / 5xx propagate instead of pretending writable or not.
        for status in [
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let stub = spawn_stub(stub_config(
                status,
                "overloaded",
                StatusCode::NO_CONTENT,
                "",
            ))
            .await;
            let err = stub.client().probe_writable().await.unwrap_err();
            match err {
                Error::Status { status: code, .. } => assert_eq!(code, status.as_u16()),
                other => panic!("expected Error::Status, got {other:?}"),
            }
        }
    }

    #[test]
    fn v1_client_uses_the_fixed_namespace_version() {
        assert_eq!(test_client().version(), V1_CACHE_VERSION);
    }
}
