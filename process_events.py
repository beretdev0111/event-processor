"""
Lightweight event processing pipeline tailored to the home assignment requirements.
developed by @beretdev0111.
"""

import json
import sys
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List, Tuple
import logging
from dataclasses import dataclass
from enum import Enum
from collections import defaultdict

# ---------- logging configuration ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ---------- data contract ----------
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
        ),  # Required, for deduplication & idempotent processing
        FieldSchema(
            name="timestamp",
            field_type=FieldType.TIMESTAMP,
            required=True,
            constraints={"format": "ISO8601"},
        ),  # Required, Can't do time-series aggregation without a time
        FieldSchema(
            name="service",
            field_type=FieldType.STRING,
            required=True,
            constraints={"min_length": 1},
        ),  # Required - for groupping metrics by service name
        FieldSchema(
            name="latency_ms",
            field_type=FieldType.FLOAT,
            required=True,
            constraints={"min": 0},
        ),  # Required, needed for system-level performance monitoring,
            # can not populate because we don't have any values to calculate it manually.
        FieldSchema(
            name="event_type",
            field_type=FieldType.STRING,
            required=True,
            constraints={"min_length": 1},
        ),  # Required, to distinguish start/complete/fail events
        FieldSchema(
            name="status_code",
            field_type=FieldType.INTEGER,
            required=False,
            constraints={"min": 100, "max": 599},
        ),  # Optional. helpful but non‑critical. Can expect from event type.
        FieldSchema(
            name="user_id",
            field_type=FieldType.STRING,
            required=False,
            constraints={"default": "unknown"},
        ),  # Optional. Useful for debugging & user-level monitring, but not critical for service-level metrics.    
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


# ---------- processor class ----------
class EventProcessor:
    """Handles loading, validation, cleaning, and aggregation."""

    def __init__(self, input_path: str) -> None:
        self.input_path = input_path
        self.raw: List[Dict] = []
        self.cleaned: List[Dict] = []
        self.quarantined: List[Dict] = []

    def load(self) -> None:
        """Read JSONL; invalid JSON is quarantined directly."""
        with open(self.input_path) as f:
            for lineno, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    obj["_line"] = lineno
                    self.raw.append(obj)
                except json.JSONDecodeError as e:
                    self.quarantined.append(
                        {"line": lineno, "reason": "invalid_json", "detail": str(e), "raw": line}
                    )
        logger.info(f"loaded {len(self.raw)} records")

    def _validate_required(self, rec: Dict) -> Tuple[bool, Optional[str]]:
        missing = [f for f in EventDataContract.required_fields() if f not in rec or rec[f] in (None, "")]
        if missing:
            return False, f"missing {', '.join(missing)}"
        return True, None

    def _validate_type(self, name: str, value) -> Tuple[bool, Optional[str]]:
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

    def clean(self) -> None:
        """Validate/normalize each raw record, quarantining failures."""
        for rec in self.raw:
            ok, reason = self._validate_required(rec)
            if not ok:
                self.quarantined.append({
                    "line": rec.get("_line"),
                    "reason": "missing_required",
                    "detail": reason,
                })
                continue

            # Per‑field type/constraint checks
            bad = False
            for k, v in list(rec.items()):
                if k.startswith("_"):
                    continue
                valid, r = self._validate_type(k, v)
                if not valid:
                    self.quarantined.append({
                        "line": rec.get("_line"),
                        "reason": "type_error",
                        "field": k,
                        "detail": r,
                    })
                    bad = True
                    break
            if bad:
                continue

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
                self.quarantined.append({
                    "line": rec.get("_line"),
                    "reason": "bad_latency",
                    "detail": rec.get("latency_ms"),
                })
                continue
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

            self.cleaned.append(clean)
        logger.info(f"cleaned {len(self.cleaned)} records, quarantined {len(self.quarantined)}")

    def aggregate(self) -> List[Dict]:
        """Compute per-service-per-minute metrics."""
        groups = defaultdict(lambda: {"count": 0, "lats": [], "errors": 0, "status_seen": 0})
        for r in self.cleaned:
            key = (r["minute_bucket"], r["service"])
            grp = groups[key]
            grp["count"] += 1
            grp["lats"].append(r["latency_ms"])
            sc = r.get("status_code")
            if sc is not None:
                grp["status_seen"] += 1
                if sc < 200 or sc >= 300:
                    grp["errors"] += 1
        result = []
        for (minute, svc), g in sorted(groups.items()):
            avg = sum(g["lats"]) / len(g["lats"])
            result.append(
                {
                    "minute": minute.strftime("%Y-%m-%d %H:%M:%S"),
                    "service": svc,
                    "request_count": g["count"],
                    "avg_latency_ms": round(avg, 2),
                    "error_rate": round((g["errors"] / g["status_seen"]) * 100, 2)
                    if g["status_seen"]
                    else None,
                }
            )
        return result

    def save_jsonl(self, records: List[Dict], path: Path) -> None:
        with open(path, "w") as f:
            for r in records:
                # convert datetimes to iso
                rcopy = r.copy()
                if isinstance(rcopy.get("timestamp"), datetime):
                    rcopy["timestamp"] = rcopy["timestamp"].isoformat()
                if isinstance(rcopy.get("minute_bucket"), datetime):
                    rcopy["minute_bucket"] = rcopy["minute_bucket"].isoformat()
                f.write(json.dumps(rcopy) + "\n")

    def save_csv(self, records: List[Dict], path: Path) -> None:
        if not records:
            return
        headers = sorted(records[0].keys())
        with open(path, "w") as f:
            f.write(",".join(headers) + "\n")
            for r in records:
                row = [str(r.get(h, "")) for h in headers]
                f.write(",".join(row) + "\n")


# ---------- command line glue ----------
def main():
    p = argparse.ArgumentParser(description="Process HomeAssignment event stream")
    p.add_argument("input_file", help="path to JSONL events")
    p.add_argument("-o", "--output-dir", default="./output", help="where to write results")
    args = p.parse_args()

    inp = Path(args.input_file)
    outd = Path(args.output_dir)
    outd.mkdir(parents=True, exist_ok=True)

    proc = EventProcessor(str(inp))
    proc.load()
    proc.clean()
    proc.save_jsonl(proc.cleaned, outd / "cleaned_events.jsonl")
    proc.save_jsonl(proc.quarantined, outd / "quarantined_events.jsonl")
    metrics = proc.aggregate()
    proc.save_jsonl(metrics, outd / "metrics.jsonl")
    proc.save_csv(metrics, outd / "metrics.csv")

    logger.info(f"results in {outd.resolve()}")


if __name__ == "__main__":
    main()
