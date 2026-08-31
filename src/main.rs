use async_stream::stream;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

use axum::response::sse::{Event, Sse};
use dotenvy::dotenv;
use flate2::{write::GzEncoder, Compression};
use futures::stream::Stream;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{
    borrow::Cow,
    convert::Infallible,
    env,
    io::{BufReader as StdBufReader, Write},
};
use tokio::{
    fs::OpenOptions,
    io::{AsyncWriteExt, BufWriter},
    net::TcpListener,
    sync::broadcast,
};
use yup_oauth2::{
    ApplicationDefaultCredentialsAuthenticator, ApplicationDefaultCredentialsFlowOpts,
    authenticator::ApplicationDefaultCredentialsTypes,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BrokerMessage<'a> {
    pub topic: Cow<'a, str>,
    pub timestamp: u64,
    pub payload: Box<RawValue>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IngestPayload {
    pub logs: Vec<Box<RawValue>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenv().ok();

    println!("Starting cloud tiered broker...");

    let bucket = env::var("GCP_BUCKET_NAME").unwrap_or_default();
    let key_path = env::var("GCP_KEY_PATH").ok().or_else(|| env::var("GOOGLE_APPLICATION_CREDENTIALS").ok());

    // Set standard GCP env var for local testing if a path was provided
    if let Some(ref path) = key_path {
        if std::path::Path::new(path).exists() {
            println!("Local Auth: Pointing to key file at {}", path);
            // SAFETY: Executed synchronously at startup before Tokio spawns worker threads
            unsafe {
                env::set_var("GOOGLE_APPLICATION_CREDENTIALS", path);
            }
        }
    }

    // Builder auto-detects local GOOGLE_APPLICATION_CREDENTIALS or falls back to VM Metadata
    let opts = ApplicationDefaultCredentialsFlowOpts::default();
    let gcp_auth = match ApplicationDefaultCredentialsAuthenticator::builder(opts).await {
        ApplicationDefaultCredentialsTypes::InstanceMetadata(auth) => {
            println!("Cloud Auth: Using GCP VM Instance Metadata server");
            auth.build()
                .await
                .expect("Failed to build metadata authenticator")
        }
        ApplicationDefaultCredentialsTypes::ServiceAccount(auth) => {
            println!("Local Auth: Using Service Account file");
            auth.build()
                .await
                .expect("Failed to build service account authenticator")
        }
    };

    let (tx, _rx) = broadcast::channel::<bytes::Bytes>(10_000);
    let mut disk_rx = tx.subscribe();

    let reqwest_client = Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?;

    /* ---------------------------------------------------------
     *                    DISK SEGMENT
     * ---------------------------------------------------------
     */
    tokio::spawn(async move {
        println!("Disk manager task running in background");
        tokio::fs::create_dir_all("logs").await.unwrap();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("logs/hot_tier.log")
            .await
            .expect("Failed to open hot_tier.log");

        let mut current_file_size = file.metadata().await.map(|m| m.len() as usize).unwrap_or(0);
        let mut writer_opt = Some(BufWriter::new(file));

        let mut last_rotation = std::time::Instant::now();
        const SEVEN_DAYS: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 60 * 60);

        loop {
            // Unblocks every 1 hour to evaluate timer during inactive/low-traffic periods
            let recv_result = tokio::time::timeout(
                std::time::Duration::from_secs(3600),
                disk_rx.recv()
            ).await;

            match recv_result {
                Ok(Ok(bytes)) => {
                    if let Some(writer) = writer_opt.as_mut() {
                        if let Err(e) = writer.write_all(&bytes).await {
                            eprintln!("Disk failed to write: {}", e);
                            continue;
                        }
                        if let Err(e) = writer.write_all(b"\n").await {
                            eprintln!("Disk failed to write newline: {}", e);
                            continue;
                        }

                        current_file_size += bytes.len() + 1;
                    } else {
                        continue;
                    }
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(missed))) => {
                    eprintln!("Disk manager fell behind broadcast buffer! Missed {} messages.", missed);
                    continue;
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                    println!("Flushing remaining logs and shutting down disk manager...");
                    if let Some(mut writer) = writer_opt.take() {
                        let _ = writer.flush().await;
                    }
                    break;
                }
                Err(_timeout) => {
                    // Hourly timeout hit — loop advances to age check
                }
            }

            // File rotation threshold check (10 MB size OR 7 days elapsed with non-empty log)
            let size_threshold = current_file_size >= 10 * 1024 * 1024;
            let time_threshold = last_rotation.elapsed() >= SEVEN_DAYS && current_file_size > 0;

            if size_threshold || time_threshold {
                let reason = if size_threshold { "10MB threshold" } else { "7-day age threshold" };
                println!("Log reached {}. Rotating and uploading...", reason);

                last_rotation = std::time::Instant::now();

                if let Some(mut old_writer) = writer_opt.take() {
                    let _ = old_writer.flush().await;
                    let old_file = old_writer.into_inner();
                    drop(old_file);
                }

                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or(std::time::Duration::ZERO)
                    .as_secs();

                let archive_name = format!("logs/archive_{}.log", timestamp);
                let cloud_name = format!("segment_{}.log.gz", timestamp);

                if let Err(e) = tokio::fs::rename("logs/hot_tier.log", &archive_name).await {
                    eprintln!("Failed to rotate log: {}", e);
                    continue;
                }

                let new_file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("logs/hot_tier.log")
                    .await
                    .expect("Failed to create fresh hot_tier.log");

                writer_opt = Some(BufWriter::new(new_file));
                current_file_size = 0;

                let upload_bucket = bucket.clone();
                let client = reqwest_client.clone();
                let auth = gcp_auth.clone();

                tokio::spawn(async move {
                    println!("Requesting GCP upload token...");
                    let scopes = &["https://www.googleapis.com/auth/devstorage.read_write"];

                    let token_str = match auth.token(scopes).await {
                        Ok(t) => match t.token() {
                            Some(tok) => tok.to_string(),
                            None => {
                                eprintln!("Upload failed for {}: missing token string.", cloud_name);
                                let _ = tokio::fs::remove_file(&archive_name).await;
                                return;
                            }
                        },
                        Err(e) => {
                            eprintln!("Upload failed for {}: auth error: {}.", cloud_name, e);
                            let _ = tokio::fs::remove_file(&archive_name).await;
                            return;
                        }
                    };

                    match compress_and_upload_log(
                        archive_name.clone(),
                        upload_bucket,
                        cloud_name.clone(),
                        client,
                        token_str,
                    )
                    .await
                    {
                        Ok(_) => println!("Segment {} securely stored in cloud", cloud_name),
                        Err(e) => {
                            eprintln!("Upload failed for segment {}: {}.", cloud_name, e);
                            let _ = tokio::fs::remove_file(&archive_name).await;
                            let _ = tokio::fs::remove_file(&format!("{}.gz", archive_name)).await;
                        }
                    }
                });
            }
        }
    });

    /* ---------------------------------------------------------
     *                PRODUCER/CONSUMER SEGMENT
     * ---------------------------------------------------------
     */
    let tx_producer = tx.clone();

    let app = Router::new()
        .route("/ingest", post(ingest_handler))
        .route("/stream", get(consumer_handler))
        .with_state(tx_producer);

    let http_listener = TcpListener::bind("0.0.0.0:8080").await?;
    println!("Axum HTTP server actively listening on 0.0.0.0:8080...");
    axum::serve(http_listener, app).await.expect("Axum server crashed");

    Ok(())
}

async fn compress_and_upload_log(
    local_filename: String,
    bucket_name: String,
    object_name: String,
    client: Client,
    token_str: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Compressing {}...", local_filename);

    let compressed_filename = format!("{}.gz", local_filename);
    let input_path = local_filename.clone();
    let output_path = compressed_filename.clone();

    tokio::task::spawn_blocking(move || {
        let input_file = std::fs::File::open(input_path)?;
        let mut reader = StdBufReader::new(input_file);

        let compressed_file = std::fs::File::create(output_path)?;
        let buf_writer = std::io::BufWriter::new(compressed_file);
        let mut encoder = GzEncoder::new(buf_writer, Compression::default());

        std::io::copy(&mut reader, &mut encoder)?;

        let mut inner_writer = encoder.finish()?;
        inner_writer.flush()?;

        Ok::<(), std::io::Error>(())
    })
    .await??;

    println!("Uploading {} to Google Cloud...", compressed_filename);
    let file = tokio::fs::File::open(&compressed_filename).await?;
    let stream = tokio_util::codec::FramedRead::new(file, tokio_util::codec::BytesCodec::new());
    let body = reqwest::Body::wrap_stream(stream);

    let url = format!(
        "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
        bucket_name, object_name
    );

    let response = client
        .post(&url)
        .bearer_auth(token_str)
        .header("Content-Type", "application/gzip")
        .body(body)
        .send()
        .await?;

    if response.status().is_success() {
        println!("Success. File {} safely stored in bucket.", object_name);
        tokio::fs::remove_file(local_filename).await?;
        tokio::fs::remove_file(&compressed_filename).await?;
        println!("Local files wiped cleanly");
        Ok(())
    } else {
        let error_msg = response.text().await?;
        Err(format!("GCP rejected the upload: {}", error_msg).into())
    }
}

async fn ingest_handler(
    State(tx): State<broadcast::Sender<bytes::Bytes>>,
    headers: HeaderMap, // <-- Extract headers from request
    Json(payload): Json<IngestPayload>,
) -> impl IntoResponse { // <-- Changed return type
    
    // 1. API Key Security Check
    let expected_key = env::var("API_SECRET_KEY").unwrap_or_else(|_| "fallback-dev-key".to_string());
    let provided_key = headers.get("X-API-Key")
        .and_then(|k| k.to_str().ok())
        .unwrap_or("");

    if provided_key != expected_key {
        return (StatusCode::FORBIDDEN, "Forbidden: Invalid API Key").into_response();
    }

    // 2. Process Payload
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(std::time::Duration::ZERO)
        .as_secs();

    for event in payload.logs {
        let broker_msg = BrokerMessage {
            topic: Cow::Borrowed("mobile_telemetry"),
            timestamp,
            payload: event,
        };

        if let Ok(json_str) = serde_json::to_string(&broker_msg) {
            let _ = tx.send(bytes::Bytes::from(json_str));
        }
    }

    StatusCode::OK.into_response()
}

async fn consumer_handler(
    State(tx): State<broadcast::Sender<bytes::Bytes>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // 1. API Key Security Check
    let expected_key = env::var("API_SECRET_KEY").unwrap_or_else(|_| "fallback-dev-key".to_string());
    let provided_key = headers.get("X-API-Key")
        .and_then(|k| k.to_str().ok())
        .unwrap_or("");

    if provided_key != expected_key {
        return (StatusCode::FORBIDDEN, "Forbidden: Invalid API Key").into_response();
    }

    // 2. Start SSE Stream
    let mut rx = tx.subscribe();
    let sse_stream = stream! {
        loop {
            match rx.recv().await {
                Ok(bytes) => {
                    if let Ok(json_str) = std::str::from_utf8(&bytes) {
                        yield Ok::<_, Infallible>(Event::default().data(json_str));
                    }
                }
                Err(_) => break,
            }
        }
    };

    Sse::new(sse_stream)
        .keep_alive(axum::response::sse::KeepAlive::default())
        .into_response()
}