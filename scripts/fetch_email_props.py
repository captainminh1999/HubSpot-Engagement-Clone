#!/usr/bin/env python3
"""
Fetch HubSpot email properties and write to CSV.
Usage: python fetch_email_props.py input.csv results.csv
"""

import csv
import os
import sys
import time
from typing import Dict, List, Optional
import requests

# Properties to fetch from the API
PROPERTIES = [
    'hs_email_status',
    'hs_email_error_message',
    'hs_email_post_send_status',
    'hs_email_message_id',
    'hs_email_thread_id',
    'hs_email_sent_via'
]

# Column order for output CSV
COLUMNS = ['emailId'] + PROPERTIES

def get_token() -> str:
    """Get HubSpot token from environment variable."""
    token = os.getenv('HUBSPOT_TOKEN')
    if not token:
        print("Error: HUBSPOT_TOKEN environment variable not set", file=sys.stderr)
        sys.exit(1)
    return token

def fetch_email_properties(email_id: str, token: str) -> Dict[str, str]:
    """
    Fetch properties for a single email ID.
    Returns dict with property values (empty strings for missing/error cases).
    """
    if not email_id.strip():
        return {prop: '' for prop in PROPERTIES}

    url = f"https://api.hubapi.com/crm/v3/objects/emails/{email_id}"
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
    }
    params = {
        'properties': ','.join(PROPERTIES)
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        
        # Handle rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', '60'))
            print(f"Rate limited. Waiting {retry_after} seconds...", file=sys.stderr)
            time.sleep(retry_after)
            # Retry the request
            response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            return {
                prop: str(data.get('properties', {}).get(prop, ''))
                for prop in PROPERTIES
            }
        else:
            print(f"Error fetching {email_id}: HTTP {response.status_code}", file=sys.stderr)
            return {prop: '' for prop in PROPERTIES}

    except requests.RequestException as e:
        print(f"Request failed for {email_id}: {e}", file=sys.stderr)
        return {prop: '' for prop in PROPERTIES}

def process_emails(input_file: str, output_file: str, token: str) -> None:
    """Process all email IDs from input CSV and write results to output CSV."""
    try:
        with open(input_file, 'r', newline='', encoding='utf-8-sig') as infile, \
             open(output_file, 'w', newline='', encoding='utf-8') as outfile:

            # Read and clean the header
            reader = csv.DictReader(infile)
            if not reader.fieldnames:
                print("Error: Input CSV is empty or has no headers", file=sys.stderr)
                sys.exit(1)
            
            # Clean up fieldnames to handle any BOM or whitespace
            fieldnames = [f.strip() for f in reader.fieldnames]
            if 'emailId' not in fieldnames:
                print(f"Error: Input CSV must have 'emailId' column. Found columns: {', '.join(fieldnames)}", file=sys.stderr)
                sys.exit(1)

            writer = csv.DictWriter(outfile, fieldnames=COLUMNS)
            writer.writeheader()

            for row in reader:
                email_id = row['emailId'].strip()
                if not email_id:
                    continue

                props = fetch_email_properties(email_id, token)
                writer.writerow({
                    'emailId': email_id,
                    **props
                })

    except FileNotFoundError as e:
        print(f"Error: File not found - {e.filename}", file=sys.stderr)
        sys.exit(1)
    except PermissionError as e:
        print(f"Error: Permission denied - {e.filename}", file=sys.stderr)
        sys.exit(1)
    except csv.Error as e:
        print(f"CSV error: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    if len(sys.argv) != 3:
        print("Usage: python fetch_email_props.py input.csv results.csv", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    token = get_token()

    process_emails(input_file, output_file, token)

if __name__ == '__main__':
    main()