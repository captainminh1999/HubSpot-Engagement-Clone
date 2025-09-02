# HubSpot Engagement Export

This script exports engagement data from HubSpot using their API.

## Features

- Exports engagement data for multiple IDs in parallel
- Handles rate limiting automatically
- Implements HubSpot's recommended retry policy (3-day retry window with exponential backoff)
- Supports both API key and private app token authentication
- Generates individual JSON files and combined outputs
- Comprehensive error handling and logging

## Setup

1. Install Python requirements:
```bash
pip install -r requirements.txt
```

2. Prepare a CSV file with engagement IDs (default name: "Engagement ID.csv")

## Usage

Basic usage with private app token:
```bash
python scripts/export_engagements.py --api-key-name hapikey --api-key-value YOUR_TOKEN
```

Additional options:
- `--limit N`: Process only N engagements
- `--concurrency N`: Set number of concurrent requests (default: 8)
- `--rate-limit N`: Set requests per second (default: 10)
- `--timeout N`: Set request timeout in seconds (default: 15)
- `--csv PATH`: Specify custom input CSV path
- `--output-dir DIR`: Specify custom output directory

## Output

The script creates:
- Individual JSON files for each engagement in the output directory
- A combined JSONL file (optional, use --jsonl)
- A combined JSON array file (optional, use --combined)
- Error logs and summary in the output directory
