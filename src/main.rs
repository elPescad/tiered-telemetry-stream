// Describe what the Stack is and what the Heap is? Which is faster for memory operations? 2pts
// Answer in the form of a comment:
/*
 * the stack is where the currently required information goes for operations (it's small but really fast).
 * the heap, however, is more like a storage container for not-so-immediate info and is much slower.
 * for this reason the stack is faster for memory operations.
 */

// What is a thread? Answer can be metaphorical, literal, or descriptive. 1pt
// Answer in the form of a comment:
/*
 * threads are essentially pieces of cores provisioned by the os to allow concurrent and async operations. 
 */

// What is a green thread? Answer can be metaphorical, literal, or descriptive. 1pt.
// Hint: The operating system has no idea it exists.
// Answer in the form of a comment:
/*
 * not really sure? im assuming they're "fake" threads that are created by async runtimes for dynamic management of async between program threads
 */
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

    // The line below is one of the worst things in this code base for effeciency. can you explain why? 5pts
 
    /* CONTEXT: In this specific line tx is a sender and rx is a reciever. tx gets the messages from the sockets 
     * which you can think of a socket as a virtual cable, and sends them to one of many recievers.
     * To further elaborate on sockets quickly. Say someone wants to send you mail. so they put your address
     * on the envelope and then put it in their specific mailbox to be sent out. It is then transported to your mailbox.
     * You can then send mail back to this person via your very same mailbox and they can recieve mail via their same mailbox
     * that is the most basic concept of a socket.
     *  
     * Think of message as a struct or a container in simplier terms, currently it stores a Json however
     * think of it as simply a raw string. We will use "HelloWorld" as the message that rx is recieving.
     * This message will then be transmitted and converted into many different form across multiple green threads and thread.
     * the tx and rx are stored on the stack however the Messages typically stored on the stack by default
     * are stored on the heap now due to broadcast::channel being a container. Containers and their contents
     * are stored in the heap.
     * The heap is allocated for 10k Messages
     */
 
    // Answer in the form of a comment: 
    /*
     * its a very large heap and the conversions between types require extra time which can make them expensive
     * and inefficient (plus potentially laggy), especially when large amounts of requests are sent all at once
     */
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

                    // How many Megabytes (MB) is this. Approximation is fine. 1pt.
                    
                    /* CONTEXT: In this program we store messages in a file, again we can think
                     * of those messages as a simple "HelloWorld," once we 10 * 1024 * 1024
                     * bytes of "HelloWorld" we save the file and rotate. This question is
                     * simply asking how many MB this is
                    */

                    //Answer in the form of a comment: 
                    /*
                     * roughly 10MB
                     */
                    if current_file_size >= 10 * 1024 * 1024 {
                        println!("Log reached threshold. rotating and uploading...");

                        let _ = writer.flush().await;

                        // This is a potential race condition. In what scenario could it be triggered. 3pts

                        /* CONTEXT: variable is simply the timestamp used to name the current file
                         * once it reaches 10 * 1024 * 1024 bytes. Once it reaches the byte limit
                         * the file is renamed based on the timestamp variable. An example would
                         * hot_tier.log would be renamed hot_tier100.log. The number 100 represents the time
                         * in second. So then every file should be unique since each file is represented in seconds
                         * only except for in a specific scenario. We use the UNIX epoch to represent seconds
                         * since January 1st 1970 which was around when the UNIX operating system was invented.
                         * So each second will be unique. I want you to test something before you answer
                         * in a linux terminal type this command: date +%s. See the result will be some big number
                         * Wait 5 seconds then type it again. Use this to answer the question.
                         */

                        // Answer in the form of a comment:
                        /*
                         * since its only second precision, id assume a race condition may occur when under heavy load.
                         * the program would name two files the same thing if they both were filled up within the same second, 
                         * which would destroy the first log file that was created
                         */
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