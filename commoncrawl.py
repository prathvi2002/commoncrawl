#!/usr/bin/env python3

# Script to get all URLs crawled by Common Crawl for a given domain (like aads.com) by querying the CDX index.

import requests, json, argparse, time
from urllib.parse import quote_plus
from rich import print

# This is the base URL for Common Crawl's CDX index API (the database of crawled URLs).
CDX_BASE = "https://index.commoncrawl.org"

def get_all_indexes():
    """Fetch all available Common Crawl index names (e.g., CC-MAIN-2024-33)."""
    try:
        resp = requests.get(f"{CDX_BASE}/collinfo.json", timeout=15)
        resp.raise_for_status()
        return [entry["id"] for entry in resp.json()]
    except requests.exceptions.RequestException as e:
        print(f"[red]❌ Failed to fetch index list:[/red] {e}")
        return []

def query_cdx(domain, index_id):
    """Query one index and return all matching URLs for the domain."""
    encoded = quote_plus(domain)
    url = f"{CDX_BASE}/{index_id}-index?url={encoded}/*&output=json"
    headers = {"User-Agent": "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0"}
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
            time.sleep(2)

    return urls

def main():
    parser = argparse.ArgumentParser(description="🌐 Get all URLs from all Common Crawl indexes for a domain")
    parser.add_argument("url", help="Domain to search (e.g., aads.com)")
    args = parser.parse_args()

    all_indexes = get_all_indexes()
    if not all_indexes:
        print("[red]❌ No indexes available. Exiting.[/red]")
        return

    print(f"[blue]🔍 Found {len(all_indexes)} indexes. Querying all for:[/blue] {args.url}")
    total_urls = set()

    for index in all_indexes:
        print(f"[bold cyan]→ Querying index:[/bold cyan] {index}")
        urls = query_cdx(args.url, index)
        print(f"[green]✓ {len(urls)} URLs found in {index}[/green]")
        total_urls.update(urls)

    print(f"\n[bold green]✅ Total unique URLs across all indexes: {len(total_urls)}[/bold green]")
    for url in sorted(total_urls):
        print(url)

if __name__ == "__main__":
    main()

