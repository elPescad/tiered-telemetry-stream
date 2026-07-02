use tokio::net::TcpListener;
use tokio::io::AsyncReadExt;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
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
use std::fs::File;
use axum::{
    routing::post,
    routing::get,
    Router,
    Json,
    extract::State,
    http::StatusCode,
};
use std::io::{Read, Write};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BrokerMessage {
    pub topic: String,
    pub timestamp: u64,
    pub payload: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TelemetryEvent {
    pub e: String,           // Event type ('v', 'a', 'se')
    pub id: Option<String>,  // Target ID (optional)
    pub t: Option<u64>,      // Duration/Value (optional)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IngestPayload {
    pub logs: Vec<TelemetryEvent>, // The array of logs from mobile
}

#[derive(Clone, Debug)]
enum Message
{
    Json(BrokerMessage),
}
//Box acts essentially as a pointer but without the need to manually dereference
//here main runs asynchronously and returns type () -> good or it returns
//a dyanmic error type as a pointer Box
#[tokio::main]

async fn main() -> Result<(), Box<dyn std::error::Error>>
{
    //load .env into file
    dotenv().ok();

    println!("Starting cloud tiered broker...");
    //the '?' lets us error check each statement

    //creates channel that can hold 100 unread messages in RAM
    //tx is transmitter
    //rx is reciever
    let (tx, _rx) = broadcast::channel::<Message>(100);
    
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
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("logs/hot_tier.log")
            .await
            .expect("Failed to open hot_tier.log");

        //wait for messages
        loop {
            match disk_rx.recv().await {
                Ok(msg) => {
                    let data = match &msg {
                        Message::Json(j) => serde_json::to_vec(j).unwrap(),
                    };

                    //write and sync
                    if let Err(e) = file.write_all(&data).await {
                        eprintln!("Disk failed to write: {}", e);
                        continue;
                    }
                    //write data
                    let _ = file.write_all(b"\n").await;
                    //drop letover bytes in kernel
                    let _ = file.sync_all().await;

                    //file rotation
                    if let Ok(meta) = tokio::fs::metadata("logs/hot_tier.log").await {
                        //10MB (10KB for now)
                        if meta.len() >= 10 * 1024 * 1024{
                            println!("Log reached threshold. rotating and uploading...");

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
                            tokio::fs::create_dir_all("logs").await.unwrap();

                            file = OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open("logs/hot_tier.log")
                                .await
                                .expect("Failed to create fresh hot_tier.log");

                            //Gets bucket and key from env file
                            let bucket = env::var("GCP_BUCKET_NAME").unwrap();
                            let key = env::var("GCP_KEY_PATH").unwrap();

                            //uploads in the background
                            tokio::spawn(async move {
                                match compress_and_upload_log(&archive_name, &bucket, &cloud_name, &key).await {
                                    Ok(_) => println!("Segment {} securely stored in cloud", cloud_name),
                                    Err(e) => eprintln!("Upload failed for segment {}: {}", cloud_name, e),
                                } 
                            });

                        }
                    }
                }
                Err(_) => {
                    continue;
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
        .route("/ingest", post(ingest_handler))// Post request to push info
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
async fn compress_and_upload_log(local_filename: &str, bucket_name: &str, object_name: &str, key_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Compressing {}...", local_filename);

    //creates new .gz file
    let compressed_filename = format!("{}.gz", local_filename);
    //anon block. Acts as sandbox to ensure coder finishes and closes file cleanly
    {
        let mut input_file = File::open(local_filename)?;
        //create new file to store compressed data whose path is the new .gz file name we created
        let compressed_file = File::create(&compressed_filename)?;
        let mut encoder = GzEncoder::new(compressed_file, Compression::default());

        //create storage for bytes
        let mut buffer = Vec::new();
        input_file.read_to_end(&mut buffer)?;
        //write stored bytes into new compression file
        encoder.write_all(&buffer)?;
        encoder.finish()?;
    }

    //Authenticate with Google Cloud using JSON key
    println!("Authenticating with GCP...");
    let secret = read_service_account_key(key_path).await?;
    let auth = ServiceAccountAuthenticator::builder(secret).build().await?;
    let scopes = &["https://www.googleapis.com/auth/devstorage.read_write"];
    let token = auth.token(scopes).await?;

    //Upload compressed file to google cloud bucket
    println!("Uploading {} to Google Cloud...", compressed_filename);
    let file_bytes = tokio::fs::read(&compressed_filename).await?;
    let client = Client::new();

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
async fn ingest_handler(State(tx): State<broadcast::Sender<Message>>, Json(payload): Json<IngestPayload>,) -> StatusCode {
    println!("Recieved batch of {} events from mobile client", payload.logs.len());

    //loop through the batched logs and forward them to existing disk writer
    for event in payload.logs {
        //convert tiny mobile data into string for storage
        let payload_str = serde_json::to_string(&event).unwrap_or_default();

        //structures log payload for redability for later access
        let broker_msg = BrokerMessage {
            topic: "mobile_telemetry".to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            payload: payload_str,
        };

        //send to original broadcast signal
        let _ = tx.send(Message::Json(broker_msg));
    }

    //Return an HTTP 200 OK so the app knows it's safe to delete its local buffer
    StatusCode::OK
}

//handler creates a persistent HTTP stream for consumer dashboard
async fn consumer_handler(State(tx): State<broadcast::Sender<Message>> ) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    println!("New consumer connected to live stream");
    let mut rx = tx.subscribe();

    //create an async stream that yields data whenever a new log arrives
    let sse_stream = stream! {
        loop {
            match rx.recv().await {
                Ok(Message::Json(json_data)) => {
                    //Convert the string to struct and push it to HTTP client
                    let data_str = serde_json::to_string(&json_data).unwrap_or_default();
                    yield Ok(Event::default().data(data_str));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    eprintln!("Consume lagged, missed {} message", missed);
                }
                Err(_) => break,
            }
        }
    };

    //return the stream, telling axum to keep the HTTP connection alive
    Sse::new(sse_stream).keep_alive(axum::response::sse::KeepAlive::default())
}