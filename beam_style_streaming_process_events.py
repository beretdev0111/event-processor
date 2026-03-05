"""
Streaming event processing pipeline (line-by-line simulation).
Developed by @Beretdev0111.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple, Generator
import logging
from dataclasses import dataclass
from enum import Enum
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class FieldType(Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    TIMESTAMP = "timestamp"


@dataclass
class FieldSchema:
    name: str
    field_type: FieldType
    required: bool
    constraints: Dict = None

    def __post_init__(self):
        if self.constraints is None:
            self.constraints = {}


class EventDataContract:
    """Data contract which defines rules for valid events."""

    SCHEMA = [
        FieldSchema(
            name="event_id",
            field_type=FieldType.STRING,
            required=True,
            constraints={"min_length": 1},
        ),
        FieldSchema(
            name="timestamp",
            field_type=FieldType.TIMESTAMP,
            required=True,
            constraints={"format": "ISO8601"},
        ),
        FieldSchema(
            name="service",
            field_type=FieldType.STRING,
            required=True,
            constraints={"min_length": 1},
        ),
        FieldSchema(
            name="latency_ms",
            field_type=FieldType.FLOAT,
            required=True,
            constraints={"min": 0},
        ),
        FieldSchema(
            name="event_type",
            field_type=FieldType.STRING,
            required=True,
            constraints={"min_length": 1},
        ),
        FieldSchema(
            name="status_code",
            field_type=FieldType.INTEGER,
            required=False,
            constraints={"min": 100, "max": 299},
        ),
        FieldSchema(
            name="user_id",
            field_type=FieldType.STRING,
            required=False,
            constraints={"default": "unknown"},
        ),
    ]

    @classmethod
    def required_fields(cls):
        return {f.name for f in cls.SCHEMA if f.required}

    @classmethod
    def optional_fields(cls):
        return {f.name for f in cls.SCHEMA if not f.required}

    @classmethod
    def schema_by_name(cls, name: str) -> Optional[FieldSchema]:
        for f in cls.SCHEMA:
            if f.name == name:
                return f
        return None


def validate_required(rec: Dict) -> Tuple[bool, Optional[str]]:
    """Check if all required fields are present and non-empty."""
    missing = [f for f in EventDataContract.required_fields() if f not in rec or rec[f] in (None, "")]
    if missing:
        return False, f"missing {', '.join(missing)}"
    return True, None


def validate_type(name: str, value) -> Tuple[bool, Optional[str]]:
    """Checking type for each field."""
    schema = EventDataContract.schema_by_name(name)
    if not schema or value is None:
        return True, None
    try:
        if schema.field_type == FieldType.STRING:
            if not isinstance(value, str):
                return False, f"expect string for {name}"
            if "min_length" in schema.constraints and len(value.strip()) < schema.constraints["min_length"]:
                return False, "too short"
        elif schema.field_type == FieldType.INTEGER:
            int(value)
        elif schema.field_type == FieldType.FLOAT:
            float(value)
        elif schema.field_type == FieldType.TIMESTAMP:
            datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception as e:
        return False, str(e)
    return True, None


def extract(input_path: str) -> Generator[Dict, None, None]:
    """
    Read JSONL file line-by-line and yield parsed events.
    Invalid JSON is skipped with a warning.
    Each event carries its line number for tracing.
    """
    with open(input_path) as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                obj["_line"] = lineno
                yield obj
            except json.JSONDecodeError as e:
                logger.warning(f"line {lineno}: invalid JSON - {e}")


# ---------- TRANSFORMER: validate and clean a single event ----------
def transform(rec: Dict) -> Tuple[Optional[Dict], Optional[Dict]]:
    """
    Validate and normalize a single event entry, returns cleaned one or quarantined one.
    Only one will be non-None, like:
    
    if validation fails, returns (None, quarantine_dict).
    if validation succeeds, returns (cleaned_dict, None).
    """
    # Check required fields
    ok, reason = validate_required(rec)
    if not ok:
        return None, {
            "line": rec.get("_line"),
            "reason": "missing_required",
            "detail": reason,
        }

    # Per-field type/constraint checks
    for k, v in list(rec.items()):
        if k.startswith("_"):
            continue
        valid, r = validate_type(k, v)
        if not valid:
            return None, {
                "line": rec.get("_line"),
                "reason": "type_error",
                "field": k,
                "detail": r,
            }

    # Normalize the cleaned record
    clean = {}
    clean["event_id"] = str(rec["event_id"]).strip()
    
    # timestamp normalization
    ts = datetime.fromisoformat(rec["timestamp"].replace("Z", "+00:00"))
    clean["timestamp"] = ts
    clean["minute_bucket"] = ts.replace(second=0, microsecond=0)
    clean["service"] = str(rec["service"]).strip().lower()
    
    # latency: cast to int gracefully
    try:
        clean["latency_ms"] = int(float(rec["latency_ms"]))
    except Exception:
        return None, {
            "line": rec.get("_line"),
            "reason": "bad_latency",
            "detail": rec.get("latency_ms"),
        }
    clean["event_type"] = str(rec["event_type"]).strip()

    # optional status_code
    if "status_code" in rec and rec["status_code"] not in (None, ""):
        try:
            sc = int(rec["status_code"])
            clean["status_code"] = sc
            clean["status_code_reason"] = "provided"
        except Exception:
            clean["status_code"] = None
            clean["status_code_reason"] = "invalid_cast"
    else:
        clean["status_code"] = None
        clean["status_code_reason"] = "missing_default"

    # optional user_id
    uid = rec.get("user_id")
    if uid in (None, ""):
        clean["user_id"] = "unknown"
        clean["user_id_reason"] = "missing_default"
    else:
        clean["user_id"] = str(uid)
        clean["user_id_reason"] = "provided"

    return clean, None


# ---------- LOADER: write records incrementally ----------
class StreamingLoader:
    """Writes clean/quarantined events and metrics to files."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # File handles kept open for incremental writes
        self.cleaned_file = open(self.output_dir / "cleaned_events.jsonl", "w")
        self.quarantined_file = open(self.output_dir / "quarantined_events.jsonl", "w")
        
        # Metrics aggregation (in-memory for now; could use windowing)
        self.groups = defaultdict(lambda: {"count": 0, "lats": [], "errors": 0, "status_seen": 0})
        self.processed_count = 0
        self.quarantined_count = 0

    def write_cleaned(self, rec: Dict) -> None:
        """Write a cleaned record and update metrics."""
        rcopy = rec.copy()
        if isinstance(rcopy.get("timestamp"), datetime):
            rcopy["timestamp"] = rcopy["timestamp"].isoformat()
        if isinstance(rcopy.get("minute_bucket"), datetime):
            rcopy["minute_bucket"] = rcopy["minute_bucket"].isoformat()
        
        self.cleaned_file.write(json.dumps(rcopy) + "\n")
        self.cleaned_file.flush()
        self.processed_count += 1
        
        # Update metrics
        key = (rec["minute_bucket"], rec["service"])
        grp = self.groups[key]
        grp["count"] += 1
        grp["lats"].append(rec["latency_ms"])
        sc = rec.get("status_code")
        if sc is not None:
            grp["status_seen"] += 1
            if sc < 200 or sc >= 300:
                grp["errors"] += 1

    def write_quarantined(self, rec: Dict) -> None:
        """Write a quarantined record."""
        self.quarantined_file.write(json.dumps(rec) + "\n")
        self.quarantined_file.flush()
        self.quarantined_count += 1

    def finalize(self) -> None:
        """Close files and compute final metrics."""
        self.cleaned_file.close()
        self.quarantined_file.close()
        
        # Compute and save metrics
        metrics = []
        for (minute, svc), g in sorted(self.groups.items()):
            avg = sum(g["lats"]) / len(g["lats"]) if g["lats"] else 0
            metrics.append({
                "minute": minute.strftime("%Y-%m-%d %H:%M:%S"),
                "service": svc,
                "request_count": g["count"],
                "avg_latency_ms": round(avg, 2),
                "error_rate": round((g["errors"] / g["status_seen"]) * 100, 2) if g["status_seen"] else None,
            })
        
        # Save metrics as JSONL
        with open(self.output_dir / "metrics.jsonl", "w") as f:
            for m in metrics:
                f.write(json.dumps(m) + "\n")
        
        # Save metrics as CSV
        if metrics:
            headers = sorted(metrics[0].keys())
            with open(self.output_dir / "metrics.csv", "w") as f:
                f.write(",".join(headers) + "\n")
                for m in metrics:
                    row = [str(m.get(h, "")) for h in headers]
                    f.write(",".join(row) + "\n")


# ---------- main ETL pipeline ----------
def main(input_file: str, output_dir: str = "./output"):
    """
    Simulate a streaming Beam pipeline:
    1. Extract: read events line-by-line
    2. Transform: validate/clean each event
    3. Load: write to files incrementally
    """
    logger.info(f"Starting streaming ETL from {input_file}")
    
    loader = StreamingLoader(Path(output_dir))
    
    try:
        # Main loop: process events one at a time
        for raw_event in extract(input_file):
            cleaned, quarantined = transform(raw_event)
            
            if cleaned:
                loader.write_cleaned(cleaned)
            else:
                loader.write_quarantined(quarantined)
        
        logger.info(f"Processed {loader.processed_count} events, quarantined {loader.quarantined_count}")
    
    finally:
        loader.finalize()
        logger.info(f"Results written to {Path(output_dir).resolve()}")


if __name__ == "__main__":
    import argparse
    
    p = argparse.ArgumentParser(description="Stream-based event processing pipeline")
    p.add_argument("input_file", help="path to JSONL events")
    p.add_argument("-o", "--output-dir", default="./output", help="where to write results")
    args = p.parse_args()
    
    main(args.input_file, args.output_dir)