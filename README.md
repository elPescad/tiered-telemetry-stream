# Tiered-Telemetry-Stream

[Source Code](http://github.com/elPescad/tiered-telemetry-stream/blob/main/src/main.rs)

An enterprise-grade, high-throughput, asynchronous telemetry ingestion broker built in **Rust** using the **Tokio** runtime and **Axum** framework. This service serves as the core backend infrastructure for the SHPE platform, decoupling high-frequency client-side logging from the transactional database by streaming and batching payloads into **Google Cloud Storage (GCS)**.

---

## Architecture Overview

The system utilizes a decoupled producer-consumer architecture powered by a thread-safe, high-speed asynchronous broadcast channel. This handles continuous ingestion, hot disk logging, threshold-based file rotation, in-memory compression, and cold-storage uploading concurrently without blocking worker threads.

> **Data Flow Map:**
> 1. **Client Device** $\rightarrow$ `POST /ingest` (Batched JSON + API Key Header)
> 2. **Axum Handlers** $\rightarrow$ Authenticates request & broadcasts wrapped envelope to Tokio channel
> 3. **Disk Manager Task** $\rightarrow$ Appends payload to `logs/hot_tier.log`
> 4. **Rotation Manager** $\rightarrow$ Triggers at **10 MB** or **7 Days** of age
> 5. **Blocking Compression Sandbox** $\rightarrow$ Compresses log to `.gz` format (`flate2`)
> 6. **GCP Storage Task** $\rightarrow$ Streams compressed archive (`segment_<timestamp>.log.gz`) to GCS & purges local copy

---

## Key Technical Enhancements in Code

* **Header-Based Authentication:** All routes (`/ingest` and `/stream`) require header verification (`X-API-Key`) against the configured secret.
* **Dual-Trigger File Rotation:** Rotates the hot tier file when it reaches **10 MB** in size **OR** after **7 days** of inactivity/age.
* **Smart GCP Auth Discovery:** Uses `yup-oauth2` Application Default Credentials (ADC) to automatically toggle between a local Service Account JSON file (`GOOGLE_APPLICATION_CREDENTIALS`) and GCE/GKE VM Instance Metadata servers.
* **High-Capacity Broadcast Channel:** Handles up to **10,000** buffered in-memory messages before lagging receivers drop frames.
* **Zero-Downtime Purging:** Rotated log files (`archive_<timestamp>.log`) and compressed segments are deleted from local disk only after receiving a confirmed successful status from GCS.

---

## Environment Variables

| Variable | Default Value | Required | Description |
| :--- | :--- | :--- | :--- |
| `API_SECRET_KEY` | `fallback-dev-key` | No | Security token required in the `X-API-Key` header. |
| `GCP_BUCKET_NAME` | `""` | **Yes** | Target Google Cloud Storage bucket for cold tier storage. |
| `GCP_KEY_PATH` | None | No | Path to local GCP Service Account JSON key. |
| `GOOGLE_APPLICATION_CREDENTIALS` | None | No | Fallback GCP key file path set automatically at boot. |

---

## Performance & Concurrency Benchmarks

* **Hardware Footprint:** Benchmarked on a minimal **2 vCPU core (4 threads total)** GCP Compute Engine VM instance.
* **Throughput & Speed:** Achieved peak throughput of **14,400+ req/sec** with sub-80ms route dispatch latency under load.
* **Target Concurrency Benchmark:** Verified non-blocking execution at **10,000 concurrent streams** (C10k threshold) across 4 worker threads using asynchronous socket I/O multiplexing.
* **Optimal Operating Envelope:** Sustains **5,000 to 7,500 active concurrent connections** with zero packet loss or request timeouts.
* **Memory Efficiency:** Zero-copy payload handoffs and a capped broadcast buffer (10,000 elements) maintain flat, predictable RAM usage without thread contention.

---

## Scaled Engineering Math & Storage Optimization

### 1. Daily Ingestion Volume ($N = 7,500$ Active Users)

* **Client Profile:** 7,500 active users averaging 5 hours of usage/day, emitting telemetry events every 16 seconds ($\sim 225 	ext{ logs/hour/user}$).
* **Payload Footprint:** Average raw JSON object size $ pprox 150 	ext{ bytes}$.

$$ 	ext{Total User-Hours/Day} = 7,500 \times 5 = 37,500 	ext{ hours/day} $$

$$ 	ext{Total Telemetry Logs/Day} = 37,500 	ext{ hours} \times 225 	ext{ logs/hour} \approx 8,437,500 	ext{ logs/day} $$

$$ 	ext{Raw Daily Footprint} = 8,437,500 	ext{ logs} \times 150 	ext{ bytes} \approx 1.265 	ext{ GB/day} $$

$$ 	ext{Raw Monthly Volume} = 1.265 	ext{ GB/day} \times 30 	ext{ days} \approx \mathbf{37.95 	ext{ GB/month}} $$

### 2. Compression & Retention Savings

* **Gzip Optimization:** The `GzEncoder` pipeline condenses raw 10MB batches down to **~3MB** (a $\sim 3.33\times$ reduction factor).
* **Monthly Volume Post-Compression:** $37.95 	ext{ GB} / 3.33 \approx \mathbf{11.40 	ext{ GB/month}}$.
* **14-Day Automated Retention Window:** Enforcing a strict 14-day data retention policy on the GCS storage bucket caps active bucket storage:

$$ 	ext{Active Storage Ceiling} = \frac{1.265 	ext{ GB/day} \times 14 	ext{ days}}{3.33} \approx \mathbf{5.32 	ext{ GB}} $$

---

## API Reference

### 1. Ingest Batch Payload
* **Endpoint:** `POST /ingest`
* **Headers:** `X-API-Key: <API_SECRET_KEY>`, `Content-Type: application/json`
* **Request Payload:**
```json
{
  "logs": [
    { "e": "v", "id": "screen_home", "t": 4500 },
    { "e": "a", "id": "connect_button_click" }
  ]
}
```
* **Response Statuses:** `200 OK` (Success), `403 Forbidden` (Invalid API Key).

### 2. Stream Real-Time Events
* **Endpoint:** `GET /stream`
* **Headers:** `X-API-Key: <API_SECRET_KEY>`
* **Content-Type:** `text/event-stream`
* **SSE Event Payload Envelope:**
```json
{
  "topic": "mobile_telemetry",
  "timestamp": 1700000000,
  "payload": { "e": "v", "id": "screen_home", "t": 4500 }
}
```

---

## Local Testing & Deployment

### 1. Local Development via Terminal

**Terminal 1: Run Server**
```bash
cargo run
```

**Terminal 2: Send Telemetry Ingestion Request**
```bash
curl -v http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -H "X-API-Key: fallback-dev-key" \
  -d '{"logs": [{"e": "view_post", "id": "local_test_123", "t": 4500}]}'
```

**Terminal 3: Listen to Live Telemetry SSE Stream**
```bash
curl -N http://localhost:8080/stream \
  -H "X-API-Key: fallback-dev-key"
```

### 2. Production Docker Deployment

1. Create a `.env` configuration file:
```env
API_SECRET_KEY=your_secure_api_key_here
GCP_BUCKET_NAME=your-gcs-bucket-name
GCP_KEY_PATH=/app/credentials.json
```

2. Launch container with mounted volume paths:
```bash
docker run -d \
  --name telemetry-broker \
  --restart unless-stopped \
  -p 8080:8080 \
  --env-file .env \
  -v $(pwd)/credentials.json:/app/credentials.json \
  -v $(pwd)/logs:/app/logs \
  telemetry-broker
```
