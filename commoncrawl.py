#!/usr/bin/env python3

# Script to get all URLs crawled by Common Crawl for a given domain (like aads.com) by querying the CDX index.

import requests, json, argparse, time
from urllib.parse import quote_plus
from rich import print
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# Base URL of the Common Crawl CDX index.
CDX_BASE = "https://index.commoncrawl.org"

def get_all_indexes(after=None, before=None):
    """Fetch and optionally filter available Common Crawl index names (e.g., CC-MAIN-2024-33)."""
    try:
        resp = requests.get(f"{CDX_BASE}/collinfo.json", timeout=15)
        resp.raise_for_status()
        all_indexes = [entry["id"] for entry in resp.json()]
    except requests.exceptions.RequestException as e:
        print(f"[red]❌ Failed to fetch index list:[/red] {e}")
        return []

    filtered = []
    for index_id in all_indexes:
        try:
            year = int(index_id.split("-")[2][:4])
            if after and year < after:
                continue  # skip years before the --after value
            if before and year >= before:
                continue  # skip years >= --before value
            filtered.append(index_id)
        except (IndexError, ValueError):
            continue

    return filtered


def query_cdx(domain, index_id):
    """Query one index and return all matching URLs for the domain."""
    encoded = quote_plus(domain)
    url = f"{CDX_BASE}/{index_id}-index?url={encoded}/*&output=json"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0"}
    urls = []

    for attempt in range(1):
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
            for line in resp.text.strip().split("\n"):
                try:
                    record = json.loads(line)
                    urls.append(record["url"])
                except json.JSONDecodeError:
                    continue
            break
        except requests.exceptions.RequestException as e:
            print(f"[yellow]⚠️ {index_id}: Attempt {attempt+1} failed:[/yellow] {e}")
            time.sleep(1)

    return urls

def main():
    parser = argparse.ArgumentParser(description="Get all URLs from all Common Crawl indexes for a domain.\nSimple usage example: commoncrawl example.com", formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("url", help="Domain to search (e.g., example.com)")
    parser.add_argument("-a", "--after", type=int, help="Only use indexes from this year onward. Example: --after 2024")
    parser.add_argument("-b", "--before", type=int, help="Only use indexes before this year. Example: --before 2026")
    parser.add_argument("-c", "--concurrency", type=int, default=1, help="Number of concurrent index queries (default: 1).\n2 concurrency is safe.")
    parser.add_argument("-o", "--output", type=str, help="File to save output URLs.")
    parser.add_argument( "-A", "--append", action="store_true", help=f"Append to the output file instead of overwriting (default: False).")
    
    args = parser.parse_args()

    if not args.url:
        print("Provide a domain name. Example: commoncrawl example.com")
        sys.exit(1)

    all_indexes = get_all_indexes(after=args.after, before=args.before)
    if not all_indexes:
        print("[red]❌ No indexes available. Exiting.[/red]")
        return

    print(f"[blue]🔍 {len(all_indexes)} matching indexes selected for:[/blue] {args.url}")
    from threading import Lock
    total_urls = []
    url_lock = Lock()

    if args.concurrency == 1:
        for index in all_indexes:
            urls = query_cdx(args.url, index)
            print(f"[green]✓ {len(urls)} URLs found in index {index}[/green]")
            total_urls.extend(urls)
    else:
        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            futures = {executor.submit(query_cdx, args.url, index): index for index in all_indexes}
            for future in as_completed(futures):
                index = futures[future]
                try:
                    urls = future.result()
                    print(f"[green]✓ {len(urls)} URLs found in index {index}[/green]")
                    with url_lock:
                        total_urls.extend(urls)
                except Exception as e:
                    print(f"[red]❌ {index} crashed:[/red] {e}")

    total_urls = list(set(total_urls))

    print(f"\n[bold green]✅ Total unique URLs across all indexes: {len(total_urls)}[/bold green]")
    for url in sorted(total_urls):
        print(url)

    if args.append:
        file_method = "a"
    else:
        file_method = "w"

    if args.output:
        output_file = os.path.expanduser(f"{args.output}")

        with open(output_file, file_method) as f:
            for url in sorted(total_urls):
                f.write(f"{url}\n")

if __name__ == "__main__":
    main()