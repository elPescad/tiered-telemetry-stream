use tokio::net::TcpListener;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::broadcast;
use yup_oauth2::{read_service_account_key, ServiceAccountAuthenticator};
use reqwest::Client;
use axum::response::sse::{Event, Sse};
use std::convert::Infallible;
use futures::stream::Stream;
use async_stream::stream;
use dotenvy::dotenv;
use serde::{Serialize, Deserialize};
use std::env;
use flate2::write::GzEncoder;
use flate2::Compression;
use axum::{
    routing::post,
    routing::get,
    Router,
    Json,
    extract::State,
    http::StatusCode,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BrokerMessage {
    pub topic: String,
    pub timestamp: u64,
    pub payload: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IngestPayload {
    pub logs: Vec<serde_json::Value>,
}

#[derive(Clone, Debug)]
enum Message {
    Json(BrokerMessage),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenv().ok();

    println!("Starting cloud tiered broker...");

    let (tx, _rx) = broadcast::channel::<Message>(10_000);
    
    let mut disk_rx = tx.subscribe();

    tokio::spawn(async move {
        println!("Disk manager task running in background");
        tokio::fs::create_dir_all("logs").await.unwrap();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("logs/hot_tier.log")
            .await
            .expect("Failed to open hot_tier.log");

        let mut writer = BufWriter::new(file);
        let mut current_file_size = 0;

        loop {
            match disk_rx.recv().await {
                Ok(msg) => {
                    let data = match &msg {
                        Message::Json(j) => serde_json::to_vec(j).unwrap(),
                    };

                    if let Err(e) = writer.write_all(&data).await {
                        eprintln!("Disk failed to write: {}", e);
                        continue;
                    }
                    let _ = writer.write_all(b"\n").await;
                    
                    current_file_size += data.len() + 1;

                    if current_file_size >= 10 * 1024 * 1024 {
                        println!("Log reached threshold. rotating and uploading...");

                        let _ = writer.flush().await;

                        let timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
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

                        writer = BufWriter::new(new_file);
                        current_file_size = 0;

                        let bucket = env::var("GCP_BUCKET_NAME").unwrap_or_default();
                        let key = env::var("GCP_KEY_PATH").unwrap_or_default();

                        tokio::spawn(async move {
                            match compress_and_upload_log(archive_name.clone(), bucket, cloud_name.clone(), key).await {
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
                Err(_) => continue,
            }
        }
    });

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

async fn compress_and_upload_log(local_filename: String, bucket_name: String, object_name: String, key_path: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Compressing {}...", local_filename);

    let compressed_filename = format!("{}.gz", local_filename);
    let local_clone = local_filename.clone();
    let comp_clone = compressed_filename.clone();

    tokio::task::spawn_blocking(move || {
        let mut input_file = std::fs::File::open(&local_clone)?;
        let compressed_file = std::fs::File::create(&comp_clone)?;
        let mut encoder = GzEncoder::new(compressed_file, Compression::default());

        std::io::copy(&mut input_file, &mut encoder)?;
        encoder.finish()?;
        Ok::<(), std::io::Error>(())
    }).await??;

    println!("Authenticating with GCP...");
    let secret = read_service_account_key(&key_path).await?;
    let auth = ServiceAccountAuthenticator::builder(secret).build().await?;
    let scopes = &["https://www.googleapis.com/auth/devstorage.read_write"];
    let token = auth.token(scopes).await?;

    println!("Uploading {} to Google Cloud...", compressed_filename);
    let file_bytes = tokio::fs::read(&compressed_filename).await?;
    
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?;

    let url = format!(
        "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
        bucket_name, object_name
    );

    let response = client
        .post(&url)
        .bearer_auth(token.token().unwrap())
        .header("Content-Type", "application/gzip")
        .body(file_bytes)
        .send()
        .await?;

    if response.status().is_success() {
        println!("Success. File {} safely stored in bucket.", object_name);

        tokio::fs::remove_file(&local_filename).await?;
        tokio::fs::remove_file(&compressed_filename).await?;
        println!("Local files wiped cleanly");
        Ok(())
    } else {
        let error_msg = response.text().await?;
        Err(format!("GCP rejected the upload: {}", error_msg).into())
    }
}

async fn ingest_handler(State(tx): State<broadcast::Sender<Message>>, Json(payload): Json<IngestPayload>) -> StatusCode {
    for event in payload.logs {
        let broker_msg = BrokerMessage {
            topic: "mobile_telemetry".to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            payload: event.to_string(),
        };

        let _ = tx.send(Message::Json(broker_msg));
    }

    StatusCode::OK
}

async fn consumer_handler(State(tx): State<broadcast::Sender<Message>>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    println!("New consumer connected to live stream");
    let mut rx = tx.subscribe();

    let sse_stream = stream! {
        loop {
            match rx.recv().await {
                Ok(Message::Json(json_data)) => {
                    let data_str = serde_json::to_string(&json_data).unwrap_or_default();
                    yield Ok(Event::default().data(data_str));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    eprintln!("Consumer lagged, missed {} message", missed);
                }
                Err(_) => break,
            }
        }
    };

    Sse::new(sse_stream).keep_alive(axum::response::sse::KeepAlive::default())
}