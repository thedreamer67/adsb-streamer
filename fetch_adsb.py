"""
fetch_adsb.py
-------------
Downloads ADS-B Exchange hires-traces for a given date/hour window and
writes hourly CSV files whose columns match the adsb_exchange Postgres table.

The script streams each per-aircraft trace file directly from the public
Cloudflare R2 bucket — no rclone or AWS CLI needed.

Usage:
    python fetch_adsb.py
    python fetch_adsb.py --date 2026-04-01 --start-hour 12 --end-hour 16
    python fetch_adsb.py --workers 20 --output-dir data

Dependencies:
    pip install boto3 tqdm

Credentials:
    Uses the public ADS-B Exchange sample credentials by default.
    To use your own, set ADSBX_ACCESS_KEY and ADSBX_SECRET_KEY in .env
    and update BUCKET accordingly.
"""

import argparse
import csv
import gzip
import io
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

try:
    import boto3
    from botocore.config import Config
except ImportError:
    print("Missing dependency. Run:  pip install boto3")
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    print("Missing dependency. Run:  pip install tqdm")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # .env is optional here; credentials fall back to public defaults


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

R2_ENDPOINT    = "https://6ff2cd7dae70306649e2c1e1500e2e0a.r2.cloudflarestorage.com"
PUBLIC_KEY_ID  = "7b8d31d9d2f2d73ffb2208614db599fa"
PUBLIC_SECRET  = "8bd955340e355418a5204e52a055005b6ca761f896abe0cccdd741638e790a76"

# Public sample bucket — replace with e.g. "adsbx-recent-hires-traces" if you have paid access
BUCKET = os.environ.get("ADSBX_BUCKET", "adsbx-sample-data")

ACCESS_KEY = os.environ.get("ADSBX_ACCESS_KEY", PUBLIC_KEY_ID)
SECRET_KEY = os.environ.get("ADSBX_SECRET_KEY", PUBLIC_SECRET)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("adsb_fetch")


# ---------------------------------------------------------------------------
# CSV columns — matches the adsb_exchange Postgres table
# ---------------------------------------------------------------------------

FIELDNAMES = [
    "timestamp",
    "hex",
    "r",
    "t",
    "type",
    "flight",
    "alt_geom",
    "tas",
    "track",
    "true_heading",
    "squawk",
    "lat",
    "lon",
]


# ---------------------------------------------------------------------------
# S3 client
# ---------------------------------------------------------------------------

def make_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


# ---------------------------------------------------------------------------
# List all trace file keys for a given date
# ---------------------------------------------------------------------------

def list_trace_keys(client, date: datetime) -> list[str]:
    """
    List all hires-trace object keys for a given UTC date.
    Path format: hires-traces/yyyy/mm/dd/xx/trace_full_<icao>.json
    """
    prefix = f"hires-traces/{date.strftime('%Y/%m/%d')}/"
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


# ---------------------------------------------------------------------------
# Parse a single trace file, return rows filtered to [start_ts, end_ts)
# ---------------------------------------------------------------------------

def parse_trace(data: bytes, start_ts: float, end_ts: float) -> list[dict]:
    """
    Parse a hires-trace JSON file (gzip compressed, no .gz extension).
    Returns rows within [start_ts, end_ts) as dicts matching FIELDNAMES.

    Trace array entry format (index → field):
      0: seconds after base timestamp
      1: lat
      2: lon
      3: alt_baro (ft or "ground" or null)
      4: ground speed (knots)
      5: track (degrees) — or true_heading when on ground
      6: flags (bitfield)
      7: spare / null
      8: dict of extended fields (alt_geom, tas, true_heading, squawk,
                                   flight, r, t, type, ...)
    """
    try:
        raw = gzip.decompress(data)
    except Exception:
        raw = data  # some files may not be compressed

    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return []

    icao        = obj.get("icao", "")
    base_ts     = float(obj.get("timestamp", 0))
    trace       = obj.get("trace", [])

    # Static fields that apply to all points in this file
    # (r, t, type are per-file metadata in hires-traces)
    file_r    = obj.get("r", None)
    file_t    = obj.get("t", None)
    file_type = obj.get("type", None)

    rows = []
    for entry in trace:
        if len(entry) < 3:
            continue

        ts = base_ts + float(entry[0])
        if ts < start_ts or ts >= end_ts:
            continue

        lat   = entry[1]
        lon   = entry[2]
        track = entry[5] if len(entry) > 5 else None

        # Extended fields dict at index 8
        ext = entry[8] if len(entry) > 8 and isinstance(entry[8], dict) else {}

        dt = datetime.fromtimestamp(ts, tz=timezone.utc)

        rows.append({
            "timestamp":    dt.isoformat(),
            "hex":          icao,
            "r":            ext.get("r", file_r),
            "t":            ext.get("t", file_t),
            "type":         ext.get("type", file_type),
            "flight":       ext.get("flight"),
            "alt_geom":     ext.get("alt_geom"),
            "tas":          ext.get("tas"),
            "track":        track,
            "true_heading": ext.get("true_heading"),
            "squawk":       ext.get("squawk"),
            "lat":          lat,
            "lon":          lon,
        })

    return rows


# ---------------------------------------------------------------------------
# CSV writer keyed by hour
# ---------------------------------------------------------------------------

class HourlyCsvManager:
    def __init__(self, output_dir: Path, date_str: str):
        self.output_dir = output_dir
        self.date_str = date_str
        self._handles: dict[int, tuple] = {}
        self._counts: dict[int, int] = {}

    def _open(self, hour: int):
        path = self.output_dir / f"adsb_{self.date_str}_hour{hour:02d}.csv"
        fh = open(path, "w", newline="", encoding="utf-8")
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        self._handles[hour] = (fh, writer)
        self._counts[hour] = 0
        log.info("Opened CSV: %s", path)

    def write_rows(self, rows: list[dict]):
        for row in rows:
            # Derive hour from timestamp string
            try:
                dt = datetime.fromisoformat(row["timestamp"])
                hour = dt.hour
            except Exception:
                continue
            if hour not in self._handles:
                self._open(hour)
            _, writer = self._handles[hour]
            writer.writerow(row)
            self._counts[hour] += 1

    def close_all(self):
        for hour, (fh, _) in self._handles.items():
            fh.flush()
            fh.close()
        total = sum(self._counts.values())
        log.info("Closed all CSV files. Total rows written: %d", total)
        for hour in sorted(self._counts):
            log.info("  hour %02d UTC -> %d rows", hour, self._counts[hour])


# ---------------------------------------------------------------------------
# Worker: download + parse one file
# ---------------------------------------------------------------------------

def process_key(key: str, start_ts: float, end_ts: float) -> list[dict]:
    client = make_client()
    try:
        resp = client.get_object(Bucket=BUCKET, Key=key)
        data = resp["Body"].read()
        return parse_trace(data, start_ts, end_ts)
    except Exception as e:
        log.debug("Failed to process %s: %s", key, e)
        return []


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Download ADS-B Exchange hires-traces for a date/hour window "
            "and write hourly CSV files matching the adsb_exchange Postgres table."
        )
    )
    parser.add_argument("--date", default="2026-04-01",
                        help="UTC date to fetch (YYYY-MM-DD). Default: 2026-04-01")
    parser.add_argument("--start-hour", type=int, default=12,
                        help="Start hour (UTC, inclusive). Default: 12")
    parser.add_argument("--end-hour", type=int, default=16,
                        help="End hour (UTC, exclusive). Default: 16")
    parser.add_argument("--output-dir", default="data",
                        help="Output directory. Default: ./data/")
    parser.add_argument("--workers", type=int, default=10,
                        help="Parallel download threads. Default: 10")
    args = parser.parse_args()

    try:
        date = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        log.error("Invalid date: %s", args.date)
        sys.exit(1)

    start_ts = date.replace(hour=args.start_hour).timestamp()
    end_ts   = date.replace(hour=args.end_hour).timestamp()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    date_str = date.strftime("%Y%m%d")
    csv_mgr = HourlyCsvManager(output_dir, date_str)

    log.info("Date       : %s", args.date)
    log.info("Window     : %02d:00 – %02d:00 UTC", args.start_hour, args.end_hour)
    log.info("Bucket     : %s", BUCKET)
    log.info("Output dir : %s", output_dir.resolve())

    client = make_client()

    log.info("Listing trace files…")
    try:
        keys = list_trace_keys(client, date)
    except Exception as e:
        log.error("Failed to list bucket contents: %s", e)
        log.error("April 1 2026 may not be available in the public sample bucket.")
        log.error("If you have a paid subscription, set ADSBX_BUCKET, ADSBX_ACCESS_KEY, ADSBX_SECRET_KEY in .env")
        sys.exit(1)

    log.info("Found %d trace files. Downloading with %d workers…", len(keys), args.workers)

    total_rows = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_key, k, start_ts, end_ts): k for k in keys}
        with tqdm(total=len(keys), unit="file") as bar:
            for future in as_completed(futures):
                rows = future.result()
                if rows:
                    csv_mgr.write_rows(rows)
                    total_rows += len(rows)
                bar.update(1)

    csv_mgr.close_all()
    log.info("Done. Total data points written: %d", total_rows)


if __name__ == "__main__":
    main()