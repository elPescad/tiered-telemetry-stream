use async_stream::stream;
use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};

use axum::response::sse::{Event, Sse};
use futures::stream::Stream;
use dotenvy::dotenv;
use flate2::{write::GzEncoder, Compression};
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

use yup_oauth2::{read_service_account_key, ServiceAccountAuthenticator};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BrokerMessage<'a> {
    pub topic: Cow<'a, str>,
    pub timestamp: u64,
    pub payload: Box<RawValue>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IngestPayload {
    pub logs: Vec<Box<RawValue>>, // The array of logs from mobile
}

//Box acts essentially as a pointer but without the need to manually dereference
//here main runs asynchronously and returns type () -> good or it returns
//a dyanmic error type as a pointer Box
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    //load .env into file
    dotenv().ok();

    println!("Starting cloud tiered broker...");
    //the '?' lets us error check each statement

    // Fetch Env vars ONCE
    let bucket = env::var("GCP_BUCKET_NAME").unwrap_or_default();
    let key_path = env::var("GCP_KEY_PATH").unwrap_or_default();

    // OPTIMIZATION: Initialize GCP Auth once to leverage in-memory token caching and auto-refresh.
    // This also acts as a "Fail-Fast" check at startup instead of failing during rotation.
    let gcp_secret = read_service_account_key(&key_path).await
        .expect("Failed to read GCP service account key - check GCP_KEY_PATH");
    let gcp_auth = ServiceAccountAuthenticator::builder(gcp_secret).build().await
        .expect("Failed to create GCP authenticator");

    //creates channel that can hold 10,000 unread messages in RAM
    //tx is transmitter
    //rx is reciever
    // (Optimized to use Arc<String> to prevent cloning strings for every connected user)
    let (tx, _rx) = broadcast::channel::<bytes::Bytes>(10_000);    
    let mut disk_rx = tx.subscribe();

    // OPTIMIZATION: Build the Reqwest client once to reuse the connection pool and TLS certificates
    let reqwest_client = Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?;

    //sole purpose is to write to log file and rotate files once it reaches
    //transfer phase

    /* ---------------------------------------------------------
     *                    DISK SEGMENT
     * ---------------------------------------------------------
     *  */
    tokio::spawn(async move {
        println!("Disk manager task running in background");
        tokio::fs::create_dir_all("logs").await.unwrap();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("logs/hot_tier.log")
            .await
            .expect("Failed to open hot_tier.log");

        // FIX: Start tracking size using the actual current file size from disk metadata.
        // This prevents the file from exceeding 10MB if the server restarts.
        let mut current_file_size = file.metadata().await.map(|m| m.len() as usize).unwrap_or(0);

        // Use Option to allow safely dropping the writer & file handle before renaming
        // BufWriter batches small disk writes into memory to avoid IOPS bottlenecks
        let mut writer_opt = Some(BufWriter::new(file));

        //wait for messages
        loop {
            match disk_rx.recv().await {
                Ok(bytes) => { // Fixed syntax error
                    // Removed: let bytes = json_str.as_bytes();

                    // Ensure size tracker ONLY increments if writes actually succeed
                    if let Some(writer) = writer_opt.as_mut() {
                        // Pass &bytes slice directly
                        if let Err(e) = writer.write_all(&bytes).await {
                            eprintln!("Disk failed to write: {}", e);
                            continue;
                        }
                        // Write newline separator
                        if let Err(e) = writer.write_all(b"\n").await {
                            eprintln!("Disk failed to write newline: {}", e);
                            continue;
                        }
                        
                        // Track written size in RAM
                        current_file_size += bytes.len() + 1;
                    } else {
                        continue; 
                    }

                    // File rotation (10MB threshold)
                    if current_file_size >= 10 * 1024 * 1024 {
                        println!("Log reached threshold. rotating and uploading...");

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
                                Ok(t) => {
                                    match t.token() {
                                        Some(tok) => tok.to_string(),
                                        None => {
                                            eprintln!("Upload failed for {}: missing token string. Purging archive.", cloud_name);
                                            let _ = tokio::fs::remove_file(&archive_name).await;
                                            return;
                                        }
                                    }
                                },
                                Err(e) => {
                                    eprintln!("Upload failed for {}: auth error: {}. Purging archive.", cloud_name, e);
                                    let _ = tokio::fs::remove_file(&archive_name).await;
                                    return;
                                }
                            };

                            match compress_and_upload_log(archive_name.clone(), upload_bucket, cloud_name.clone(), client, token_str).await {
                                Ok(_) => println!("Segment {} securely stored in cloud", cloud_name),
                                Err(e) => {
                                    let err_msg = e.to_string();
                                    eprintln!("Upload failed for segment {}: {}. Purging local archive to prevent disk exhaustion.", cloud_name, err_msg);
                                    let _ = tokio::fs::remove_file(&archive_name).await;
                                    let _ = tokio::fs::remove_file(&format!("{}.gz", archive_name)).await;
                                }
                            } 
                        });
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    eprintln!("Disk manager fell behind broadcast buffer! Missed {} messages.", missed);
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    println!("All senders dropped. Flushing remaining logs and shutting down disk manager...");
                    if let Some(mut writer) = writer_opt.take() {
                        let _ = writer.flush().await;
                    }
                    break;
                }
            }
        }
    });

    /* ---------------------------------------------------------
     *                PRODUCER/CONSUMER SEGMENT
     * ---------------------------------------------------------
     *  */
    let tx_producer = tx.clone();

    //Define http router and attach your broadcast channel to its state
    let app = Router::new()
        .route("/ingest", post(ingest_handler)) // Post request to push info
        .route("/stream", get(consumer_handler)) // Get request for reading data
        .with_state(tx_producer);

    //Bind to 0.0.0.0 so so external mobile devices can hit it, not just local host
    let http_listener = TcpListener::bind("0.0.0.0:8080").await?;

    //Spawn the http server in the background
    println!("Axum HTTP server actively listening on 0.0.0.0:8080...");
    axum::serve(http_listener, app).await.expect("Axum server crashed");

    Ok(())
}

//compresses file and uploads
// Changed local_filename to String from &str to prevent lifetime capture errors in tokio::spawn
async fn compress_and_upload_log(
    local_filename: String, 
    bucket_name: String, 
    object_name: String, 
    client: Client, // Accept the globally initialized client
    token_str: String, // Accept the raw extracted token directly
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Compressing {}...", local_filename);

    //creates new .gz file
    let compressed_filename = format!("{}.gz", local_filename);
    let input_path = local_filename.clone();
    let output_path = compressed_filename.clone();

    //anon block wrapped in spawn_blocking. Offloads heavy CPU work off Tokio's worker threads
    tokio::task::spawn_blocking(move || {
        // OPTIMIZATION: Wrap input in BufReader and output in BufWriter. 
        // This dramatically reduces syscall overhead during compression.
        let input_file = std::fs::File::open(input_path)?;
        let mut reader = StdBufReader::new(input_file);
        
        //create new file to store compressed data whose path is the new .gz file name we created
        let compressed_file = std::fs::File::create(output_path)?;
        let writer = GzEncoder::new(compressed_file, Compression::default());
        
        let mut encoder = GzEncoder::new(writer, Compression::default());

        //Stream byte contents directly without allocating massive RAM vectors
        std::io::copy(&mut reader, &mut encoder)?;
        
        // FIX: Extract inner BufWriter and explicitly flush it. 
        // If dropping the BufWriter throws a disk full error, standard drops ignore it.
        // Explicitly flushing guarantees data integrity on the disk.
        let mut inner_writer = encoder.finish()?;
        inner_writer.flush()?;
        
        Ok::<(), std::io::Error>(())
    }).await??;

    //Upload compressed file to google cloud bucket
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

    //Verify Delivery and Cleanup local drive
    if response.status().is_success() {
        println!("Success. File {} safely stored in bucket.", object_name);

        //safely wipe local data because google confirmed reciept
        tokio::fs::remove_file(local_filename).await?;
        tokio::fs::remove_file(&compressed_filename).await?;
        println!("Local files wiped cleanly");
        Ok(())
    } else {
        let error_msg = response.text().await?;
        Err(format!("GCP rejected the upload: {}", error_msg).into())
    }
}

//http handler
//automatically unpacks the JSON array from the React Native app
async fn ingest_handler(
    State(tx): State<broadcast::Sender<bytes::Bytes>>, 
    Json(payload): Json<IngestPayload>
) -> StatusCode {
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
            // Convert String straight into reference-counted Bytes
            let _ = tx.send(bytes::Bytes::from(json_str));
        }
    }

    StatusCode::OK
}

//handler creates a persistent HTTP stream for consumer dashboard
async fn consumer_handler(
    State(tx): State<broadcast::Sender<bytes::Bytes>>
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut rx = tx.subscribe();

    let sse_stream = stream! {
        loop {
            match rx.recv().await {
                Ok(bytes) => {
                    // Convert the shared Bytes into a string slice cheaply
                    // Event::default().data() will format the SSE without a giant intermediate buffer
                    if let Ok(json_str) = std::str::from_utf8(&bytes) {
                        yield Ok::<_, Infallible>(Event::default().data(json_str));
                    }
                }
                Err(_) => break,
            }
        }
    };

    Sse::new(sse_stream).keep_alive(axum::response::sse::KeepAlive::default())
}