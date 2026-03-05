# Event Processing Pipeline

A lightweight, modular event processing pipeline for system-level monitoring. 
Validates, cleans, and aggregates events from file and outputs cleaned, quarantined event lists as JSON, and metrics as csv.

---

# Part 1: Event Processing & Cleaning

## Principles

### 1. **Data Contract**

#### Critical Fields (quarantine if missing)
- **`event_id`**: Needed to uniquely identify each event, deduplication & idempotent processing
- **`timestamp`**: Can't do time-series aggregation without knowing when events happened
- **`service`**: We group metrics by service name, so this is essential
- **`latency_ms`**: Essential for system-level performance monitoring
- **`event_type`**: Lets us distinguish start/complete/fail events

#### Optional Fields (populate with defaults)
- **`status_code`**: HTTP status, helpful for error rate but non-critical (defaults to `None`)
- **`user_id`**: Useful for debugging, not for basic aggregates (defaults to `"unknown"`)

### 2. **Validation & Cleaning Strategy**

Events flow through three stages:

1. **Parse**: Read JSONL; invalid JSON is quarantined immediately
2. **Validate**: Check required fields present, types correct, constraints satisfied
3. **Clean**: Normalize timestamps, safely cast latency to int, fill optional fields with defaults and reasons

**What gets quarantined and why:**
- Missing critical fields → can't compute reliable metrics
- Bad timestamp format → breaks time-series ordering
- Non-numeric latency → we track latency, so it must be a number
- Type mismatches (e.g., string status code that won't parse) → prevents downstream errors

**What stays and gets defaults:**
- Missing `status_code` → assume unknown (doesn't block error rate if we have enough data)
- Missing `user_id` → default to `"unknown"` (useful for debugging, not for core metrics)
- Missing optional event_type → default to `"unknown"` (allows classification later)

### 3. **Outputs**

The pipeline produces three output files per run:

1. **`cleaned_events.jsonl`**: Valid, normalized events ready for analysis
2. **`quarantined_events.jsonl`**: Invalid/incomplete events with reason codes (for audit/debugging)
3. **`metrics.jsonl` + `metrics.csv`**: Aggregated metrics (request count, avg latency, error rate) per service per minute

## Usage

### Prerequisites
```bash
# Python 3.7+, no external dependencies for the core script
python3 --version
```

### Basic Usage
```bash
python3 process_events.py /path/to/events.jsonl -o ./output
```

### With Custom Output Directory
```bash
python3 process_events.py /path/to/events.jsonl --output-dir /some/other/path
```

### Example
```bash
python3 process_events.py \
  "/home/beret-dev/work/Test/Data/HomeAssignmentEvents.jsonl" \
  -o ./output
```

### Output
```
output/
├── cleaned_events.jsonl        # ~1940 records (95%+ of input)
├── quarantined_events.jsonl    # ~78 records (bad data, audit trail)
├── metrics.jsonl               # Per-service-per-minute aggregates
└── metrics.csv                 # Same metrics in CSV format
```

## Metrics Explained

Each metric record contains:

| Field | Example | Meaning |
|-------|---------|---------|
| `minute` | `2025-01-12 09:05:00` | Time bucket (rounded to minute) |
| `service` | `checkout` | Service name |
| `request_count` | 5 | Requests in this minute |
| `avg_latency_ms` | 156.4 | Mean latency (ms) |
| `error_rate` | 20.0 | % of requests with non-2xx status code |

## Error Handling

| Reason | Example | Recovery |
|--------|---------|----------|
| `missing_required` | Missing `timestamp` | Re-request event from source |
| `type_error` | `status_code: "abc"` | Fix upstream to send valid type |
| `bad_latency` | `latency_ms: "not a number"` | Inspect event, correct source format |
| `processing_error` | Rare; internal bug | Check logs, file issue |
| `invalid_json` | Malformed JSON line | Fix JSON encoder upstream |

## File Structure

```
/home/beret-dev/work/Test/
├── process_events.py                                   # Main pipeline (batch/local)
├── beam_style_streaming_process_events.py              # beam-style pipeline (batch/local)
├── fastapi_metrics_api.py                              # fastAPI based API server
├── simple_metrics_api.py                               # Metrics API server
└── README.md                                           # This file
```

---

# Part 2: Data Storage Architecture

1. **Raw event data**
Normally, Raw data is stored in the Landing zone, Amazon S3 or google cloud storate is best solution. Maybe, if you want to combine data lake and data warehouse in a platform (for easy-to-analyze), we can use delta lake over s3 or GCS.

Raw data will not stored in Snowflake or Bigquery, because it will has schema evolution risk (log data schema maybe changs.) Also, snowflake or bigquery is too expensive for storing raw data.

**How I will store:**
I will build streaming pipeline, which store raw event data as parquet file format, which is partitioned by timestamp(year, month, date). We can use Apache spark/pyspark here.

2. **Aggregated metrics will be stored in BigQuery (analytics) and Bigtable (serving)**, 

In BigQuery, store data in partitioned table by date(from timestamp), if volume gets high, then add clustering by "service" or "event_type"
In Bigtable, set row key as "service#event_type#timestamp_reverse" (Maybe different from filtering - most frequent access(filtering) pattern)

3. **Schema evolution over time:**
- In the storage where the raw data is stored (s3): Simply adding new columns - old files simply fill null value. 
- In analytics database : Just add new columns, not dropping fields or changing data types. If multiple fields are changed or added, then create new table.
- In bigtable: no migration is needed. Just add new columns.

Summary: schema evolution is handled by 
1. Store raw data in the flexible parquet format, 
2. Allow additive changes in analytics warehouse,
3. Use schema-light databases for serving layers. 

---

# Part 3: Metrics API

## Setup & Installation

### ⚠️ Prerequisites

Before starting the metrics API, you **must first run the event processing pipeline** to generate cleaned events:

```bash
python3 process_events.py /path/to/HomeAssignmentEvents.jsonl -o ./output
```

This creates the required `output/cleaned_events.jsonl` file that the API reads from.

### Installation

The basic `simple_metrics_api.py` uses Python's standard library only—**no external dependencies required**.

**Just run:**
```bash
cd /home/beret-dev/work/Test
python3 simple_metrics_api.py
```

No installation step needed. Go directly to "Starting the Server" below.

---

## FastAPI Server (Recommended)

For a more robust, production-ready API with automatic documentation and better performance, use the FastAPI-based server.

### Installation & Setup

#### 1. Create Virtual Environment
```bash
# Install system dependencies (if not already installed)
sudo apt update && sudo apt install -y python3.12-venv

# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate
```

#### 2. Install Dependencies
```bash
# Install required packages
pip install -r requirements.txt
# Or manually: pip install fastapi uvicorn
```

#### 3. Run Event Processing (if not already done)
```bash
python3 process_events.py Data/HomeAssignmentEvents.jsonl -o output
```

#### 4. Launch FastAPI Server
```bash
# With virtual environment activated
python3 fastapi_metrics_api.py
```

The server will start at `http://localhost:8000` with automatic OpenAPI documentation at `http://localhost:8000/docs`.

### FastAPI API Endpoints

The FastAPI server provides the same functionality as the simple server but with enhanced features:

- **Automatic API Documentation**: Visit `http://localhost:8000/docs` for interactive Swagger UI
- **Type Validation**: Automatic request/response validation
- **Better Error Handling**: Structured error responses
- **Performance**: Async support for better concurrency

#### Base URL
```
http://localhost:8000
```

#### 1. Root / Info
```
GET /
```
Returns API information and available endpoints with usage examples.

#### 2. Query Metrics (Main Endpoint)
```
GET /metrics
```

Same filtering capabilities as the simple API, but with automatic parameter validation.

**Query Parameters:**
- `service` (string): Filter by service name
- `event_type` (string): Filter by event type  
- `user_id` (string): Filter by user ID
- `from_time` (string): Start timestamp in ISO8601 format
- `to_time` (string): End timestamp in ISO8601 format
- `limit` (integer): Max results to return (default 1000)

#### 3. Summary Statistics
```
GET /summary
```
Returns aggregate statistics about all loaded events.

### FastAPI Usage Examples

#### Start the Server
```bash
# Activate virtual environment
source .venv/bin/activate

# Launch server
python3 fastapi_metrics_api.py
```

#### Query Examples
```bash
# Get API info
curl http://localhost:8000/

# Filter by service
curl "http://localhost:8000/metrics?service=checkout"

# Filter by time range
curl "http://localhost:8000/metrics?from_time=2025-01-12T09:00:00+00:00&to_time=2025-01-12T10:00:00+00:00"

# Combined filters
curl "http://localhost:8000/metrics?service=payments&event_type=request_completed&limit=50"

# Get summary statistics
curl http://localhost:8000/summary
```

---

## API Endpoints

### Base URL
```
http://localhost:8000
```

### 1. Root / Info
```
GET /
```
Returns API information and available endpoints.

**Example:**
```bash
curl http://localhost:8000/
```

### 2. Query Metrics (Main Endpoint)
```
GET /metrics
```

Query filtered events with optional filters for service, event type, user, and timestamp range.

**Query Parameters:**

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `service` | string | Filter by service name | `checkout`, `payments`, `auth`, `search`, `catalog` |
| `event_type` | string | Filter by event type | `request_started`, `request_completed`, `request_failed` |
| `user_id` | string | Filter by user ID | `u78`, `guest166`, `svc221` |
| `from_time` | ISO8601 | Start timestamp (inclusive) | `2025-01-12T09:00:00+00:00` |
| `to_time` | ISO8601 | End timestamp (inclusive) | `2025-01-12T10:00:00+00:00` |
| `limit` | integer | Max results to return (default 1000, max 10000) | `100` |

**Response:**
```json
{
  "count": 5,
  "total_matching": 42,
  "limit": 1000,
  "filters": {
    "service": "checkout",
    "event_type": null,
    "user_id": null,
    "from_time": null,
    "to_time": null
  },
  "data": [
    {
      "event_id": "8e47ec9b",
      "timestamp": "2025-01-12T11:07:37+00:00",
      "service": "checkout",
      "latency_ms": 69,
      "status_code": 204,
      "event_type": "request_started",
      "user_id": "svc221"
    }
  ]
}
```

### 3. Summary Statistics
```
GET /summary
```
Returns aggregate statistics about all loaded events.

**Example:**
```bash
curl http://localhost:8000/summary
```

---

## Query Examples

### Example 1: All Events from a Specific Service
```bash
curl "http://localhost:8000/metrics?service=checkout"
```
Returns all checkout service events.

### Example 2: Filter by Time Range
```bash
curl "http://localhost:8000/metrics?from_time=2025-01-12T09:00:00+00:00&to_time=2025-01-12T10:00:00+00:00&limit=50"
```
Returns up to 50 events within the specified hour.

### Example 3: Specific Service + Event Type
```bash
curl "http://localhost:8000/metrics?service=payments&event_type=request_completed"
```
Returns only completed payment requests.

### Example 4: Filter by User
```bash
curl "http://localhost:8000/metrics?user_id=u78"
```
Returns all events for user `u78`.

### Example 5: Combined Filters (Service + Event Type + Time Range)
```bash
curl "http://localhost:8000/metrics?service=auth&event_type=request_failed&from_time=2025-01-12T09:00:00+00:00&to_time=2025-01-12T11:00:00+00:00&limit=100"
```
Returns up to 100 failed auth requests within a 2-hour window.

### Example 6: Summary Statistics
```bash
curl http://localhost:8000/summary
```
Returns:
- Total event count
- Event counts by service
- Event counts by event type
- Timestamp range (min/max)

---

# Part 4: Code Quality & Reliability

## Production Monitoring

I am going to monitor both of Data Quality Metrics and System health.
Here are the metrics I want to get for each monitoring.

### Data Quality Metrics
Track the integrity and health of incoming event data:

| Metric | Purpose | Action |
|--------|---------|--------|
| Invalid event rate | % of events quarantined (critical field failures) | Detect upstream schema changes |
| Timestamp parse failures | % of events with unparseable timestamps | Validate producer clock sync |
| Optional field null rate | % of missing `user_id`, `status_code` | Understand data completeness |
| Error rate per service | Non-2xx response % by service | Identify failing microservices |
| Event volume per minute | Throughput trend tracking | Detect traffic anomalies |

These metrics help detect upstream producer issues, schema drift, and service degradation early.

### Processing & System Metrics
Monitor pipeline performance and infrastructure health:

| Metric | Purpose | Target |
|--------|---------|--------|
| Processing latency | Time from ingestion → aggregation | < 5 seconds |
| Memory usage | Peak RAM consumption during run | Scale with input size |
| CPU utilization | Processing efficiency | < 80% during batch |
| API response latency | Query time for `/metrics` endpoint | < 100ms for typical queries |
| API error rate | % of failed API requests | < 0.1% |

---

## Scaling Challenges

### 1. In-Memory Processing
**Current bottleneck:** Batch processing loads entire JSONL into memory.

**Solution path:**
- Switch to streaming framework (Apache Beam, Dataflow)
- Process events line-by-line with incremental state
- Flush aggregates periodically instead of accumulating

### 2. Aggregation Performance
**Current bottleneck:** Simple group-by logic becomes slow as event volume, services, and time windows grow.

**Solution path:**
- Use partitioned storage (by date, service)
- Implement incremental aggregation (rolling windows)
- Maintain pre-aggregated tables (hourly/daily snapshots)
- Index by timestamp + service for fast lookups

### 3. API Scalability
**Current bottleneck:** Reading from file in memory is not production-ready.

**Solution path:**
- Move cleaned events to backend database (BigQuery, PostgreSQL)
- Add caching layer (Redis) for hot queries
- Implement pagination with cursor-based scrolling
- Set strict time range limits (e.g., max 7 days per query)
- Use connection pooling for database access

---

## Next Improvements

1. **Stronger data contracts** — I will update using explicit schema definitions (Pydantic models)
2. **Automated testing** — Add unit tests for validation logic
3. **Streaming optimization** — I will refactor pipeline to simulate streaming for Beam compatibility. Currently loads all events into memory and process it; this is not suitable for streaming. To optimize for streaming, events should be processed line by line - this is just Extractor, and Extractor, tranformer will be combined in beam (for example) pipeline context.

I've built updated version - optimized for beam-style streaming processing: beam_style_streaming_processing_pipeline.py





