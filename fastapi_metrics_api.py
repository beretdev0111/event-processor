"""
FastAPI server to expose cleaned events & metrics.
Developed by @Beretdev0111.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

app = FastAPI(
    title="Event Metrics API",
    description="REST API for querying cleaned event data",
    version="1.0.0"
)

EVENTS: List[Dict] = []


def load_events(file_path: Path) -> None:
    """Load cleaned events from JSONL file."""
    global EVENTS
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        EVENTS = []
        return

    EVENTS = []
    with open(file_path) as f:
        for line in f:
            try:
                event = json.loads(line)
                # Parse timestamp back to datetime for filtering
                if "timestamp" in event:
                    event["timestamp_dt"] = datetime.fromisoformat(event["timestamp"])
                EVENTS.append(event)
            except json.JSONDecodeError as e:
                logger.warning(f"Skipped malformed line: {e}")
    logger.info(f"Loaded {len(EVENTS)} events from {file_path}")


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "service": "Event Metrics API",
        "endpoints": {
            "/": "This page",
            "/metrics": "Query filtered events",
            "/summary": "Summary stats"
        },
        "events_loaded": len(EVENTS),
        "usage": {
            "GET /metrics?service=checkout": "Filter by service",
            "GET /metrics?from_time=2025-01-12T09:00:00+00:00&to_time=2025-01-12T10:00:00+00:00": "Filter by time range",
            "GET /metrics?user_id=u78": "Filter by user",
            "GET /metrics?event_type=request_completed": "Filter by event type"
        }
    }


@app.get("/metrics")
async def get_metrics(
    service: Optional[str] = Query(None, description="Filter by service name"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    from_time: Optional[str] = Query(None, description="Filter from timestamp (ISO format)"),
    to_time: Optional[str] = Query(None, description="Filter to timestamp (ISO format)"),
    limit: int = Query(1000, description="Maximum number of results to return")
):
    """Query filtered events with optional parameters."""

    # Parse timestamps
    from_dt = None
    to_dt = None

    if from_time:
        try:
            from_dt = datetime.fromisoformat(from_time)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid from_time format: {from_time}")

    if to_time:
        try:
            to_dt = datetime.fromisoformat(to_time)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid to_time format: {to_time}")

    # Filter events
    filtered = [
        e for e in EVENTS
        if (service is None or e.get("service", "").lower() == service.lower())
        and (event_type is None or e.get("event_type", "").lower() == event_type.lower())
        and (user_id is None or e.get("user_id") == user_id)
        and (from_dt is None or e.get("timestamp_dt", datetime.min) >= from_dt)
        and (to_dt is None or e.get("timestamp_dt", datetime.max) <= to_dt)
    ]

    # Sort by timestamp descending
    filtered.sort(key=lambda x: x.get("timestamp_dt", datetime.min), reverse=True)

    # Limit results
    results = filtered[:limit]

    # Clean up internal fields
    for r in results:
        r.pop("timestamp_dt", None)

    return {
        "count": len(results),
        "total_matching": len(filtered),
        "limit": limit,
        "filters": {
            "service": service,
            "event_type": event_type,
            "user_id": user_id,
            "from_time": from_time,
            "to_time": to_time
        },
        "data": results
    }


@app.get("/summary")
async def get_summary():
    """Get summary statistics."""
    if not EVENTS:
        raise HTTPException(status_code=404, detail="No events loaded")

    services = {}
    event_types = {}
    timestamp_min = None
    timestamp_max = None

    for event in EVENTS:
        # Count by service
        svc = event.get("service", "unknown")
        services[svc] = services.get(svc, 0) + 1

        # Count by event type
        evt_type = event.get("event_type", "unknown")
        event_types[evt_type] = event_types.get(evt_type, 0) + 1

        # Track timestamp range
        ts_dt = event.get("timestamp_dt")
        if ts_dt:
            if timestamp_min is None or ts_dt < timestamp_min:
                timestamp_min = ts_dt
            if timestamp_max is None or ts_dt > timestamp_max:
                timestamp_max = ts_dt

    return {
        "total_events": len(EVENTS),
        "services": services,
        "event_types": event_types,
        "timestamp_range": {
            "min": timestamp_min.isoformat() if timestamp_min else None,
            "max": timestamp_max.isoformat() if timestamp_max else None
        }
    }


def main():
    """Main function to load events and start server."""
    possible_paths = [
        Path("./output/cleaned_events.jsonl"),
        Path("output/cleaned_events.jsonl"),
    ]

    file_path = None
    for p in possible_paths:
        if p.exists():
            file_path = p
            logger.info(f"Found cleaned events at: {p}")
            break

    if file_path is None:
        logger.error("cleaned_events.jsonl not found")
        logger.info(f"Searched: {possible_paths}")
        logger.info("\nFirst, run: python3 process_events.py /path/to/HomeAssignmentEvents.jsonl -o ./output")
        exit(1)

    # Load events
    load_events(file_path)

    # Start server
    logger.info("Starting FastAPI server...")
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()