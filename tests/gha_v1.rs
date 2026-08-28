//! Integration tests for the v1 (`_apis/artifactcache`) client against the
//! behavioral fake backend (`tests/support/fake_gha.rs`).
//!
//! Gitea and Forgejo speak this REST surface instead of Twirp; the fake
//! serves both from the same state, so a v1-written entry is visible to the
//! same blob/lookup machinery the v2 tests exercise.

mod support;

use bytes::Bytes;

use hestia::gha::blob;
use hestia::gha::client::{DownloadUrl, Reservation, V1_CACHE_VERSION};
use hestia::gha::v1::V1Client;
use support::fake_gha::FakeGha;

/// Reserve + upload + commit one entry through the v1 client.
async fn store_v1(v1: &V1Client, http: &reqwest::Client, key: &str, data: &[u8]) {
    let Reservation::Created { token } = v1.create_cache_entry(key).await.unwrap() else {
        panic!("entry {key} unexpectedly already exists");
    };
    v1.upload_and_finalize(http, key, token, Bytes::copy_from_slice(data))
        .await
        .unwrap();
}

#[tokio::test]
async fn blob_round_trip() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let v1 = fake.v1(&http);

    // 1 MiB of patterned data: large enough to be a realistic pack blob.
    let data: Vec<u8> = (0..1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    store_v1(&v1, &http, "pack-roundtrip", &data).await;

    let DownloadUrl::Hit { url, matched_key } =
        v1.get_download_url("pack-roundtrip", &[]).await.unwrap()
    else {
        panic!("expected hit");
    };
    assert_eq!(matched_key, "pack-roundtrip");

    let downloaded = blob::get(&http, &url, None).await.unwrap();
    assert_eq!(downloaded.as_ref(), data.as_slice());
}

#[tokio::test]
async fn already_exists_dedup() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let v1 = fake.v1(&http);

    store_v1(&v1, &http, "pack-dedup", b"chunk data").await;

    // Reserving the same content-addressed key again must be a clean
    // AlreadyExists (status 409 alone decides), not an error.
    let reservation = v1.create_cache_entry("pack-dedup").await.unwrap();
    assert_eq!(reservation, Reservation::AlreadyExists);

    // The 409 body models the real service's exception shape.
    let response = http
        .post(format!("{}/_apis/artifactcache/caches", fake.base_url))
        .bearer_auth("fake-runtime-token")
        .json(&serde_json::json!({
            "key": "pack-dedup",
            "version": V1_CACHE_VERSION,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["typeKey"], "ArtifactCacheItemAlreadyExistsException");

    // Reservation-blocks-forever: a reservation that was never finalized
    // also answers 409.
    let Reservation::Created { .. } = v1.create_cache_entry("pack-pending").await.unwrap() else {
        panic!("expected fresh reservation");
    };
    let reservation = v1.create_cache_entry("pack-pending").await.unwrap();
    assert_eq!(reservation, Reservation::AlreadyExists);

    // But unfinalized entries are not downloadable.
    let lookup = v1.get_download_url("pack-pending", &[]).await.unwrap();
    assert_eq!(lookup, DownloadUrl::Miss);
}

#[tokio::test]
async fn download_miss_and_restore_key_prefix() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let v1 = fake.v1(&http);

    assert_eq!(
        v1.get_download_url("no-such-key", &[]).await.unwrap(),
        DownloadUrl::Miss
    );

    store_v1(&v1, &http, "m#1", b"manifest v1").await;
    store_v1(&v1, &http, "m#2", b"manifest v2").await;

    // Prefix restore key returns the newest matching entry.
    let DownloadUrl::Hit { matched_key, url } = v1.get_download_url("m#", &["m#"]).await.unwrap()
    else {
        panic!("expected hit");
    };
    assert_eq!(matched_key, "m#2");
    let data = blob::get(&http, &url, None).await.unwrap();
    assert_eq!(data.as_ref(), b"manifest v2");
}

#[tokio::test]
async fn miss_via_204_and_via_empty_200_body() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let v1 = fake.v1(&http);

    // Default: miss as HTTP 204.
    assert_eq!(
        v1.get_download_url("no-such-key", &[]).await.unwrap(),
        DownloadUrl::Miss
    );

    // Some services answer "nothing here" as a 200 with an empty body.
    fake.set_v1_miss_as_empty(&http, true).await;
    assert_eq!(
        v1.get_download_url("no-such-key", &[]).await.unwrap(),
        DownloadUrl::Miss
    );

    // Hits are unaffected by the toggle.
    store_v1(&v1, &http, "pack-visible", b"data").await;
    let DownloadUrl::Hit { url, matched_key } =
        v1.get_download_url("pack-visible", &[]).await.unwrap()
    else {
        panic!("expected hit");
    };
    assert_eq!(matched_key, "pack-visible");
    let data = blob::get(&http, &url, None).await.unwrap();
    assert_eq!(data.as_ref(), b"data");
}

#[tokio::test]
async fn read_only_reserve_makes_probe_report_not_writable() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let v1 = fake.v1(&http);

    assert!(v1.probe_writable().await.unwrap());

    fake.deny_v1_writes();

    // A read-only token answers 403 on reserve, so the probe reports
    // unwritable instead of failing.
    assert!(!v1.probe_writable().await.unwrap());
    let error = v1.create_cache_entry("pack-x").await.unwrap_err();
    assert!(matches!(error, hestia::gha::Error::WriteDenied { .. }));
}

#[tokio::test]
async fn commit_size_mismatch_surfaces_an_error() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let v1 = fake.v1(&http);

    let Reservation::Created { token } = v1.create_cache_entry("pack-mismatch").await.unwrap()
    else {
        panic!("expected reservation");
    };
    let url = format!("{}/_apis/artifactcache/caches/{token}", fake.base_url);

    // 5 bytes uploaded, but the commit declares a different size: 400 and
    // the entry stays unfinalized (lookups keep missing).
    let patched = http
        .patch(&url)
        .bearer_auth("fake-runtime-token")
        .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
        .header(reqwest::header::CONTENT_RANGE, "bytes 0-4/*")
        .body(Bytes::from_static(b"hello"))
        .send()
        .await
        .unwrap();
    assert!(patched.status().is_success());

    let commit = http
        .post(&url)
        .bearer_auth("fake-runtime-token")
        .json(&serde_json::json!({ "size": 999 }))
        .send()
        .await
        .unwrap();
    assert_eq!(commit.status(), reqwest::StatusCode::BAD_REQUEST);

    assert_eq!(
        v1.get_download_url("pack-mismatch", &[]).await.unwrap(),
        DownloadUrl::Miss
    );
}

#[tokio::test]
async fn chunked_upload_over_32mib_lands_all_bytes() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let v1 = fake.v1(&http);

    let Reservation::Created { token } = v1.create_cache_entry("pack-big").await.unwrap() else {
        panic!("expected reservation");
    };
    let url = format!("{}/_apis/artifactcache/caches/{token}", fake.base_url);

    // PATCH without a Bearer token is refused and must not corrupt the entry.
    let unauthorized = http
        .patch(&url)
        .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
        .header(reqwest::header::CONTENT_RANGE, "bytes 0-0/*")
        .body(Bytes::from_static(b"x"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);

    // >32 MiB forces two PATCH chunks (the client's chunk size is 32 MiB).
    // The first PATCH fails mid-upload; the client must retry it, and the
    // fake must append every chunk's Content-Range bytes verbatim.
    let data: Vec<u8> = (0..(32 * 1024 * 1024 + 100_000) as u32)
        .map(|i| (i % 251) as u8)
        .collect();
    fake.fail_v1_patches(&http, 1).await;
    v1.upload_and_finalize(&http, "pack-big", token, Bytes::from(data.clone()))
        .await
        .unwrap();

    let DownloadUrl::Hit { url, matched_key } = v1.get_download_url("pack-big", &[]).await.unwrap()
    else {
        panic!("expected hit");
    };
    assert_eq!(matched_key, "pack-big");
    let downloaded = blob::get(&http, &url, None).await.unwrap();
    assert_eq!(downloaded.as_ref(), data.as_slice());
}
