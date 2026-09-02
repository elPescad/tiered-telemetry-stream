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

## ⚡ Performance & High-Concurrency Benchmarks

Tested with `wrk` on Arch Linux against a budget **2 vCPU core (4 threads total)** GCP Compute Engine VM instance running Linux kernel network stack optimizations (**Google BBR congestion control + Fair Queueing `fq`**).

### **Peak Verified Metrics (C27.5k Threshold)**

| Metric | Peak Verified Result |
| :--- | :--- |
| **Sustained Concurrency** | **27,500 active, open TCP connections** |
| **Throughput** | **24,691.38 requests/sec** (~743,000 logs processed in 30s) |
| **Average Latency** | **199.43 ms** |
| **Connection Drop Rate** | **0.00%** (`connect 0` errors across 27,500 simultaneous streams) |
| **Request Success Rate** | **99.87%** (742,946 / 743,780 requests successfully served) |

---

### **Stress Test Scaling Curve**

| Active Connections | Req/Sec | Avg Latency | Connect Errors | Success Rate |
| :--- | :--- | :--- | :--- | :--- |
| **10,000 (C10k)** | 21,297 | 331.63 ms | 0 | 99.45% |
| **15,000 (C15k)** | 22,936 | 282.42 ms | 0 | 99.04% |
| **20,000 (C20k)** | 22,250 | 261.43 ms | 0 | 99.14% |
| **25,000 (C25k)** | 23,989 | 262.89 ms | 0 | 99.73% |
| **27,500 (C27.5k)** | **24,691** | **199.43 ms** | **0** | **99.87%** |
| **30,000 (C30k)** | 22,745 | 248.25 ms | 1,792 *(Kernel TCP Backlog limit)* | 99.41% |

---

### **Architectural Performance Drivers**

* **Deferred Deserialization:** Utilizes `serde_json::value::RawValue` to validate outer JSON boundaries without allocating heap memory or parsing AST trees on the hot route path.
* **Lock-Free Pipeline:** Axum route handlers validate headers and dispatch atomic payload pointers (`bytes::Bytes`) directly into a lock-free `tokio::sync::broadcast` ring buffer.
* **Kernel Network Pacing:** Pairs Linux **Google BBR** TCP congestion control with Fair Queueing (`fq`) to eliminate socket bufferbloat and prevent worker thread stalls under heavy packet load.
* **Isolated Background Workers:** Disk append writes, CPU-heavy Gzip compression (`tokio::task::spawn_blocking`), and GCS uploads run completely decoupled from incoming HTTP worker threads.

---

## Scaled Engineering Math & Free Tier Optimization

### **Dual Architectural Constraints**
This service is governed by two independent limits:
1. **Hardware Ingestion Limit:** Capable of sustaining **27,500 concurrent connections** (~24.7k req/sec) during real-time traffic spikes.
2. **Financial Storage Limit:** Modeled around **GCP's Always Free Tier (5 GB Cloud Storage)**. While network bandwidth can handle far more users, stream compression is the governing operational constraint that ensures **$0.00/month** infrastructure running costs.

---

### 1. Theoretical Zero-Cost Capacity Model ($N = 7,500$ Active Users)

* **Baseline Load Profile:** 7,500 simulated active users averaging 5 hours of usage/day, emitting telemetry events every 16 seconds ($\sim 225 \text{ logs/hour/user}$).
* **Payload Footprint:** Average raw JSON object size $\approx 150 \text{ bytes}$.

$$\text{Total User-Hours/Day} = 7,500 \times 5 = 37,500 \text{ hours/day}$$

$$\text{Total Telemetry Logs/Day} = 37,500 \text{ hours} \times 225 \text{ logs/hour} \approx 8,437,500 \text{ logs/day}$$

$$\text{Raw Daily Footprint} = 8,437,500 \text{ logs} \times 150 \text{ bytes} \approx 1.265 \text{ GB/day}$$

$$\text{Raw Monthly Volume} = 1.265 \text{ GB/day} \times 30 \text{ days} \approx \mathbf{37.95 \text{ GB/month}}$$

---

### 2. Compression & GCP Free Tier Ceiling

* **Gzip Compression Efficiency:** The worker pipeline's `GzEncoder` condenses raw 10MB log batches down to **~3MB** (a $\sim 3.33\times$ compression factor).
* **Monthly Volume Post-Compression:** $37.95 \text{ GB} / 3.33 \approx \mathbf{11.40 \text{ GB/month}}$.
* **14-Day Automated Retention Window:** Enforcing a strict 14-day lifecycle retention policy on the target GCS bucket caps maximum active stored data:

$$\text{Active Storage Ceiling} = \frac{1.265 \text{ GB/day} \times 14 \text{ days}}{3.33} \approx \mathbf{5.32 \text{ GB}}$$

> **Key takeaway:** Without Gzip stream compression, 7,500 users would generate **~17.7 GB** of retained data over 14 days, breaching GCP's 5 GB Always Free tier within 4 days. High-ratio compression allows the broker to maximize user capacity while staying strictly within zero-cost storage quotas.

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
