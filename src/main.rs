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
    pub logs: Vec<serde_json::Value>, // The array of logs from mobile
}

#[derive(Clone, Debug)]
enum Message {
    Json(BrokerMessage),
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

    //creates channel that can hold 10,000 unread messages in RAM
    //tx is transmitter
    //rx is reciever
    let (tx, _rx) = broadcast::channel::<Message>(10_000);
    
    let mut disk_rx = tx.subscribe();

    //sole purpose is to write to log file and rotate files once it reaches
    //transfer phase

    /* ---------------------------------------------------------
     *                     DISK SEGMENT
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

        // BufWriter batches small disk writes into memory to avoid IOPS bottlenecks
        let mut writer = BufWriter::new(file);
        let mut current_file_size = 0;

        //wait for messages
        loop {
            match disk_rx.recv().await {
                Ok(msg) => {
                    let data = match &msg {
                        Message::Json(j) => serde_json::to_vec(j).unwrap(),
                    };

                    //write to memory buffer
                    if let Err(e) = writer.write_all(&data).await {
                        eprintln!("Disk failed to write: {}", e);
                        continue;
                    }
                    //write data
                    let _ = writer.write_all(b"\n").await;
                    
                    // Track written size in RAM instead of hitting disk metadata on every iteration
                    current_file_size += data.len() + 1;

                    //file rotation (10MB threshold)
                    if current_file_size >= 10 * 1024 * 1024 {
                        println!("Log reached threshold. rotating and uploading...");

                        // Flush remaining memory bytes to disk before rotating
                        let _ = writer.flush().await;

                        //get time since Unix epoch to get unique file name for every file
                        let timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs();

                        //creates new names in order to store unique values in cloud
                        //name of file to compress
                        let archive_name = format!("logs/archive_{}.log", timestamp);
                        //name of file to upload
                        let cloud_name = format!("segment_{}.log.gz", timestamp);

                        //renames files
                        if let Err(e) = tokio::fs::rename("logs/hot_tier.log", &archive_name).await {
                            eprintln!("Failed to rotate log: {}", e);
                            continue;
                        }

                        //Opens new file to write
                        let new_file = OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open("logs/hot_tier.log")
                            .await
                            .expect("Failed to create fresh hot_tier.log");

                        writer = BufWriter::new(new_file);
                        current_file_size = 0;

                        //Gets bucket and key from env file
                        let bucket = env::var("GCP_BUCKET_NAME").unwrap_or_default();
                        let key = env::var("GCP_KEY_PATH").unwrap_or_default();

                        //uploads in the background
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
async fn compress_and_upload_log(local_filename: String, bucket_name: String, object_name: String, key_path: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Compressing {}...", local_filename);

    //creates new .gz file
    let compressed_filename = format!("{}.gz", local_filename);
    let local_clone = local_filename.clone();
    let comp_clone = compressed_filename.clone();

    //anon block wrapped in spawn_blocking. Offloads heavy CPU work off Tokio's worker threads
    tokio::task::spawn_blocking(move || {
        let mut input_file = std::fs::File::open(&local_clone)?;
        //create new file to store compressed data whose path is the new .gz file name we created
        let compressed_file = std::fs::File::create(&comp_clone)?;
        let mut encoder = GzEncoder::new(compressed_file, Compression::default());

        //Stream byte contents directly without allocating massive RAM vectors
        std::io::copy(&mut input_file, &mut encoder)?;
        encoder.finish()?;
        Ok::<(), std::io::Error>(())
    }).await??;

    //Authenticate with Google Cloud using JSON key
    println!("Authenticating with GCP...");
    let secret = read_service_account_key(&key_path).await?;
    let auth = ServiceAccountAuthenticator::builder(secret).build().await?;
    let scopes = &["https://www.googleapis.com/auth/devstorage.read_write"];
    let token = auth.token(scopes).await?;

    //Upload compressed file to google cloud bucket
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

    //Verify Delivery and Cleanup local drive
    if response.status().is_success() {
        println!("Success. File {} safely stored in bucket.", object_name);

        //safely wipe local data because google confirmed reciept
        tokio::fs::remove_file(&local_filename).await?;
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
async fn ingest_handler(State(tx): State<broadcast::Sender<Message>>, Json(payload): Json<IngestPayload>) -> StatusCode {
    //loop through the batched logs and forward them to existing disk writer
    for event in payload.logs {
        //convert tiny mobile data into string for storage & structures log payload for readability for later access
        let broker_msg = BrokerMessage {
            topic: "mobile_telemetry".to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            payload: event.to_string(),
        };

        //send to original broadcast signal
        let _ = tx.send(Message::Json(broker_msg));
    }

    //Return an HTTP 200 OK so the app knows it's safe to delete its local buffer
    StatusCode::OK
}

//handler creates a persistent HTTP stream for consumer dashboard
async fn consumer_handler(State(tx): State<broadcast::Sender<Message>>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    println!("New consumer connected to live stream");
    let mut rx = tx.subscribe();

    //create an async stream that yields data whenever a new log arrives
    let sse_stream = stream! {
        loop {
            match rx.recv().await {
                Ok(Message::Json(json_data)) => {
                    //Convert the struct to string and push it to HTTP client
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

    //return the stream, telling axum to keep the HTTP connection alive
    Sse::new(sse_stream).keep_alive(axum::response::sse::KeepAlive::default())
}