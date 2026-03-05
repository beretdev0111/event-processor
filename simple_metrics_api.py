"""
Simple HTTP server to expose cleaned events using Python stdlib only.
Developed by Beretdev0111.

Usage:
  python3 simple_metrics_api.py
  
Then visit: http://localhost:8000/metrics?service=checkout
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import threading

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
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


class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP request handler for metrics API."""

    def do_GET(self):
        """Handle GET requests."""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        query_params = parse_qs(parsed_path.query)
        
        # Clean up query params (parse_qs returns lists)
        params = {k: v[0] if v else None for k, v in query_params.items()}
        
        try:
            if path == "/":
                response = self.handle_root()
            elif path == "/metrics":
                response = self.handle_metrics(params)
            elif path == "/summary":
                response = self.handle_summary()
            else:
                response = {"error": "Not found"}
                self.send_response(404)
        except Exception as e:
            response = {"error": str(e)}
            self.send_response(500)
        
        self.send_json_response(response)
    
    def handle_root(self) -> Dict:
        """Root endpoint."""
        self.send_response(200)
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
    
    def handle_health(self) -> Dict:
        """Health check."""
        self.send_response(200)
        return {
            "status": "healthy",
            "events_available": len(EVENTS)
        }
    
    def handle_metrics(self, params: Dict) -> Dict:
        """Query filtered events."""
        self.send_response(200)
        
        # Extract filters
        service = params.get("service")
        event_type = params.get("event_type")
        user_id = params.get("user_id")
        from_time = params.get("from_time")
        to_time = params.get("to_time")
        limit = int(params.get("limit", 1000))
        
        # Parse timestamps
        from_dt = None
        to_dt = None
        
        if from_time:
            try:
                from_dt = datetime.fromisoformat(from_time)
            except ValueError:
                return {"error": f"Invalid from_time format: {from_time}"}
        
        if to_time:
            try:
                to_dt = datetime.fromisoformat(to_time)
            except ValueError:
                return {"error": f"Invalid to_time format: {to_time}"}
        
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
    
    def handle_summary(self) -> Dict:
        """Get summary statistics."""
        self.send_response(200)
        
        if not EVENTS:
            return {"error": "No events loaded"}
        
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
    
    def send_json_response(self, data: Dict) -> None:
        """Send JSON response."""
        self.send_header("Content-type", "application/json")
        self.end_headers()
        response = json.dumps(data, indent=2, default=str)
        self.wfile.write(response.encode())


def run_server(host: str = "0.0.0.0", port: int = 8000):
    server = HTTPServer((host, port), MetricsHandler)
    logger.info(f"Starting server at http://{host}:{port}")
    logger.info(f"Visit http://localhost:{port}/ for API info")
    logger.info(f"Query metrics: http://localhost:{port}/metrics?service=checkout")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Server stopped")
        server.shutdown()


if __name__ == "__main__":
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
    
    # Run server
    run_server()
