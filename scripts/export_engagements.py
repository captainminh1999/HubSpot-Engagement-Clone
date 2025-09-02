#!/usr/bin/env python3
"""
Export engagement JSON bodies for IDs listed in a CSV file.

Two modes:
 - fetch: perform HTTP GET against a configurable URL template to retrieve JSON for each ID
 - generate: produce placeholder JSON for each ID (offline use)

Outputs per-ID JSON files under the output directory, plus optional combined outputs.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import logging
import os
import sys
import random
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from requests import Response  # for type hints

try:
    import requests
except Exception:
    requests = None  # type: ignore


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export engagement JSON bodies for IDs in a CSV file")
    p.add_argument("--csv", default="Engagement ID.csv", help="CSV file with engagement IDs")
    p.add_argument("--output-dir", default="engagement_jsons", help="Directory to write per-ID JSON files")
    p.add_argument("--mode", choices=("fetch", "generate"), default="fetch", help="fetch from API or generate placeholders")
    p.add_argument("--url-template", 
                  default="https://api.hubapi.com/engagements/v1/engagements/{id}",
                  help="URL template to fetch an engagement JSON. Use {id} as placeholder.")
    p.add_argument("--auth-token", help="Bearer token for Authorization header")
    p.add_argument("--api-key-name", default="hapikey", help="If the API uses an API key as a query parameter, the parameter name")
    p.add_argument("--api-key-value", help="Value for the API key query parameter")
    p.add_argument("--concurrency", type=int, default=8, help="Number of concurrent workers")
    p.add_argument("--retries", type=int, default=3, help="Retries per request")
    p.add_argument("--timeout", type=float, default=15.0, help="HTTP request timeout in seconds")
    p.add_argument("--jsonl", action="store_true", help="Also create a JSONL file (one JSON object per line)")
    p.add_argument("--combined", action="store_true", help="Also create a combined JSON array file (may be large)")
    p.add_argument("--skip-existing", action="store_true", help="Skip IDs that already have output files")
    p.add_argument("--user-agent", default="hubspot-engagement-export/1.0", help="User-Agent header for HTTP requests")
    p.add_argument("--limit", type=int, help="Limit the number of engagements to process")
    p.add_argument("--rate-limit", type=float, default=10.0, 
                  help="Maximum requests per second (default: 10, HubSpot's default is 10)")
    return p.parse_args()


def read_ids_from_csv(path: str) -> List[str]:
    if not os.path.exists(path):
        logging.error("CSV file not found: %s", path)
        sys.exit(2)

    ids: List[str] = []
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return []

        id_col = None
        for i, h in enumerate(header):
            if h and h.strip().lower() in ("id", "engagement_id", "engagementid"):
                id_col = i
                break
        if id_col is None:
            id_col = 0
            if header[id_col].strip().isdigit():
                ids.append(header[id_col].strip())

        for row in reader:
            if len(row) <= id_col:
                continue
            val = row[id_col].strip()
            if val:
                ids.append(val)

    return ids


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


class RateLimiter:
    def __init__(self, rate_limit: float):
        self.rate_limit = rate_limit
        self.last_call = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.time()
            min_interval = 1.0 / self.rate_limit
            elapsed = now - self.last_call
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            self.last_call = time.time()

class HubSpotError:
    def __init__(self, status_code: int, message: str, response_body: Optional[str] = None, 
                 headers: Optional[Dict[str, str]] = None):
        self.status_code = status_code
        self.message = message
        self.response_body = response_body
        self.headers = headers or {}
        self.timestamp = datetime.utcnow().isoformat() + "Z"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status_code": self.status_code,
            "message": self.message,
            "response_body": self.response_body,
            "headers": dict(self.headers),
            "timestamp": self.timestamp
        }

def handle_error_response(resp: Response, id: str) -> HubSpotError:
    """Handle different types of error responses and return structured error info."""
    status = resp.status_code
    try:
        body = resp.json()
        message = body.get('message', body.get('error', {}).get('message', 'Unknown error'))
    except (ValueError, AttributeError):
        body = resp.text
        message = f"HTTP {status}"
    
    if status == 400:
        message = f"Bad Request - Invalid engagement ID or parameters: {message}"
    elif status == 401:
        message = f"Unauthorized - Invalid authentication credentials: {message}"
    elif status == 403:
        message = "Forbidden - Insufficient permissions or API key scope"
    elif status == 404:
        message = f"Engagement {id} not found"
    elif status == 429:
        message = "Rate limit exceeded"
    elif status >= 500:
        message = f"HubSpot API error (HTTP {status}): {message}"
    
    return HubSpotError(status, message, str(body), dict(resp.headers))

def calculate_retry_delay(attempt: int) -> float:
    """Calculate retry delay following HubSpot's policy.
    
    - Starts at 1 minute
    - Exponential backoff up to 8 hours
    - Allows up to 3 days of retries
    - Adds jitter to prevent thundering herd
    """
    # Base delay starts at 1 minute (60 seconds)
    base_delay = 60 * (2 ** attempt)  # exponential backoff
    
    # Cap at 8 hours (28800 seconds)
    base_delay = min(base_delay, 28800)
    
    # Add jitter (±10%)
    jitter = random.uniform(-0.1, 0.1)
    delay = base_delay * (1 + jitter)
    
    return delay

def fetch_engagement(id: str, url_template: str, headers: Dict[str, str], params: Dict[str, str], 
                    timeout: float, retries: int, rate_limiter: Optional[RateLimiter] = None) -> Optional[Dict[str, Any]]:
    if requests is None:
        raise RuntimeError("requests library is required for fetch mode; install using 'pip install -r requirements.txt'")

    url = url_template.format(id=id)
    attempt = 0
    start_time = time.time()
    max_retry_time = 3 * 24 * 60 * 60  # 3 days in seconds
    last_error = None

    while True:
        # Check if we've exceeded 3-day retry window
        if time.time() - start_time > max_retry_time:
            if last_error:
                logging.error('Giving up on id %s after 3 days of retries. Last error: %s', 
                            id, last_error.message)
                return {"id": id, "error": last_error.to_dict()}
            return None

        if rate_limiter:
            rate_limiter.wait()

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            
            # Check rate limit headers (HubSpot specific)
            remaining = resp.headers.get('X-HubSpot-RateLimit-Remaining', 
                                      resp.headers.get('X-RateLimit-Remaining'))
            if remaining and int(remaining) < 1:
                retry_after = float(resp.headers.get('X-RateLimit-Reset', 
                                                   resp.headers.get('Retry-After', 60)))
                logging.warning('Rate limit reached. Waiting %s seconds...', retry_after)
                time.sleep(retry_after)
                continue

            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError as e:
                    error = HubSpotError(200, f"Invalid JSON response: {str(e)}", resp.text)
                    last_error = error
                    logging.error('Invalid JSON for id %s: %s', id, error.message)
                    return {"id": id, "error": error.to_dict()}
            
            # Handle different error scenarios
            error = handle_error_response(resp, id)
            last_error = error
            
            if resp.status_code == 429 or resp.status_code >= 500:  # Rate limit or server errors
                # These are retryable according to HubSpot
                delay = calculate_retry_delay(attempt)
                logging.warning('%s (id: %s) - retrying in %.1f seconds (attempt %d)', 
                              error.message, id, delay, attempt + 1)
                time.sleep(delay)
                attempt += 1
                continue
            
            elif resp.status_code in (400, 404):  # Bad request or Not found
                logging.error('%s (id: %s)', error.message, id)
                return {"id": id, "error": error.to_dict()}
            
            elif resp.status_code in (401, 403):  # Auth issues
                logging.error('%s (id: %s)', error.message, id)
                # Auth errors usually mean we should stop entirely
                raise RuntimeError(f"Authentication error: {error.message}")
            
            else:  # Other errors
                logging.warning('Unexpected status %d for id %s: %s', 
                              resp.status_code, id, error.message)

        except requests.RequestException as e:
            # Network errors are retryable
            error = HubSpotError(0, f"Request failed: {str(e)}")
            last_error = error
            logging.warning('Request error for id %s: %s', id, str(e))
            
            delay = calculate_retry_delay(attempt)
            logging.info('Retrying id %s in %.1f seconds (attempt %d)...', 
                        id, delay, attempt + 1)
            time.sleep(delay)
            attempt += 1
            continue

    # If we got here, all retries failed
    if last_error:
        error_dict = last_error.to_dict()
        logging.error('Failed to fetch id %s after %d retries: %s', 
                     id, retries, last_error.message)
        return {"id": id, "error": error_dict}
    
    return None


def generate_placeholder(id: str) -> Dict[str, Any]:
    return {
        "id": id,
        "placeholder": True,
        "exported_at": datetime.utcnow().isoformat() + "Z",
    }


def write_json(path: str, obj: Any) -> None:
    tmp = path + ".part"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def worker(id: str, args: argparse.Namespace, headers: Dict[str, str], params: Dict[str, str], rate_limiter: Optional[RateLimiter] = None) -> Optional[Dict[str, Any]]:
    out_path = os.path.join(args.output_dir, f"{id}.json")
    if args.skip_existing and os.path.exists(out_path):
        logging.debug('Skipping existing %s', out_path)
        try:
            with open(out_path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return None

    if args.mode == "fetch":
        data = fetch_engagement(id, args.url_template, headers, params, args.timeout, args.retries, rate_limiter)
        if data is None:
            return None
        write_json(out_path, data)
        return data
    else:
        data = generate_placeholder(id)
        write_json(out_path, data)
        return data


def main() -> None:
    args = parse_args()
    
    # Set up logging to both console and file
    log_file = os.path.join(args.output_dir or ".", "export_errors.log")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),  # Console
            logging.FileHandler(log_file)  # File
        ]
    )

    ids = read_ids_from_csv(args.csv)
    logging.info('Found %d IDs in %s', len(ids), args.csv)
    if not ids:
        logging.warning('No IDs found; exiting')
        return

    ensure_dir(args.output_dir)

    headers: Dict[str, str] = {"User-Agent": args.user_agent}
    params: Dict[str, str] = {}
    # Check if the API key looks like a private app token (starts with 'pat-')
    if args.api_key_value and args.api_key_value.startswith('pat-'):
        headers["Authorization"] = f"Bearer {args.api_key_value}"
    elif args.auth_token:
        headers["Authorization"] = f"Bearer {args.auth_token}"
    elif args.api_key_name and args.api_key_value:
        params[args.api_key_name] = args.api_key_value

    if args.mode == "fetch" and not args.url_template:
        logging.error('In fetch mode, --url-template is required')
        sys.exit(2)

    results: List[Dict[str, Any]] = []
    error_count = 0
    auth_error = False
    
    jsonl_path = os.path.join(args.output_dir, "engagements.jsonl") if args.jsonl else None
    if jsonl_path:
        open(jsonl_path, "w", encoding="utf-8").close()
        
    # Also create an error summary file
    error_summary_path = os.path.join(args.output_dir, "error_summary.json")

    # Apply limit if specified
    if args.limit and args.limit > 0:
        ids = ids[:args.limit]
        logging.info('Limited to first %d IDs', args.limit)

    # Create rate limiter if in fetch mode
    rate_limiter = RateLimiter(args.rate_limit) if args.mode == "fetch" else None
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futures = {
            ex.submit(
                worker, 
                id, 
                args,
                headers, 
                params
            ): id for id in ids
        }
        
        completed = 0
        for fut in concurrent.futures.as_completed(futures):
            id = futures[fut]
            try:
                data = fut.result()
                if data is None:
                    logging.debug('No data for id %s', id)
                    continue
                
                results.append(data)
                if jsonl_path:
                    with open(jsonl_path, "a", encoding="utf-8") as jfh:
                        jfh.write(json.dumps(data, ensure_ascii=False) + "\n")
                
                completed += 1
                if completed % 100 == 0:  # Progress update every 100 items
                    logging.info('Processed %d/%d engagements (%.1f%%)', 
                               completed, len(ids), (completed * 100.0 / len(ids)))
                    
            except Exception as e:
                logging.exception('Worker failed for id %s: %s', id, e)

    if args.combined:
        combined_path = os.path.join(args.output_dir, "engagements.json")
        write_json(combined_path, results)
        logging.info('Wrote combined JSON with %d items to %s', len(results), combined_path)

    # Write error summary
    errors = [r["error"] for r in results if "error" in r]
    if errors:
        write_json(error_summary_path, {
            "total_processed": len(results),
            "error_count": len(errors),
            "errors": errors
        })
        logging.warning('Found %d errors. See %s for details', 
                       len(errors), error_summary_path)

    logging.info('Completed; %d items exported to %s (%d succeeded, %d failed)', 
                 len(results), args.output_dir, 
                 len(results) - len(errors), len(errors))


if __name__ == "__main__":
    main()
