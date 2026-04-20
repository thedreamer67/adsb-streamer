# ADSB Data Fetcher and Writer
Python script to stream data from adsbexchange and save to hourly CSV files.

## Setup
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements-min.txt

# Only if you have paid access and your own keys
cp .env.example .env
# Update the env vars in .env
```

## Usage
```bash
# Timestamps are in UTC
python fetch_adsb.py --date 2026-04-01 --start-hour 12 --end-hour 16
```