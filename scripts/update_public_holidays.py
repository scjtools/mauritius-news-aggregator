"""
update_public_holidays.py

Scrapes public holiday dates for Mauritius from officeholidays.com
and writes them to data/public_holidays.yaml.

Run via GitHub Actions on the 1st of each month, or manually:
  python scripts/update_public_holidays.py
"""

import requests
import re
import sys
from datetime import datetime, timezone
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

OUTPUT_PATH = "data/public_holidays.yaml"

MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}

# Only include actual public holidays, not observances
PUBLIC_HOLIDAY_TYPES = {"national holiday", "public holiday", "national"}


def scrape_year(year: int) -> list:
    url = f"https://www.officeholidays.com/countries/mauritius/{year}"
    print(f"Fetching: {url}")
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    holidays = []

    # officeholidays.com renders a plain <table> with no class
    # Columns: Day | Date | Holiday Name | Type | Comments
    tables = soup.find_all("table")
    target_table = None
    for table in tables:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if "date" in headers and "holiday name" in headers:
            target_table = table
            break

    if not target_table:
        print("WARNING: Could not find holidays table. The page structure may have changed.")
        print("Manual update required — check https://publicholidays.mu/")
        sys.exit(1)

    for row in target_table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        # Columns: Day(0) | Date(1) | Name(2) | Type(3) | Comments(4)
        date_text = cells[1].get_text(strip=True)   # e.g. "Jan 01"
        name_text = cells[2].get_text(strip=True)
        type_text = cells[3].get_text(strip=True).lower()

        # Skip non-public-holiday entries (observances, optional days etc.)
        if not any(t in type_text for t in PUBLIC_HOLIDAY_TYPES):
            continue

        # Parse "Jan 01", "Mar 12" etc.
        match = re.match(r"([A-Za-z]{3})\s+(\d{1,2})", date_text)
        if not match:
            continue

        month_str = match.group(1).title()[:3]
        day = int(match.group(2))
        month = MONTHS.get(month_str)
        if not month:
            continue

        try:
            date_iso = f"{year}-{month:02d}-{day:02d}"
            holidays.append({
                "date": date_iso,
                "name": name_text
            })
        except ValueError:
            continue

    if not holidays:
        print("WARNING: Table found but no public holidays extracted.")
        print("Manual update required — check https://publicholidays.mu/")
        sys.exit(1)

    holidays.sort(key=lambda x: x["date"])
    return holidays


def write_yaml(holidays: list, path: str):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with open(path, "w") as f:
        f.write("# Mauritius public holidays\n")
        f.write("# Source: officeholidays.com/countries/mauritius\n")
        f.write("# Updated automatically on the 1st of each month.\n")
        f.write("# To update manually: Actions \u2192 Update public holidays \u2192 Run workflow\n")
        f.write(f"# Last updated: {now}\n\n")
        # Group by year — take year from first entry
        years = sorted(set(h["date"][:4] for h in holidays))
        for year in years:
            year_holidays = [h for h in holidays if h["date"].startswith(year)]
            f.write(f"year: {year}\n")
            f.write("holidays:\n")
            for h in year_holidays:
                f.write(f'  - date: "{h["date"]}"\n')
                f.write(f'    name: "{h["name"]}"\n')
            f.write("\n")

    print(f"Written {len(holidays)} holidays to {path}")


def main():
    today = datetime.now()
    current_year = today.year
    next_year = current_year + 1

    all_holidays = []

    for year in [current_year, next_year]:
        try:
            holidays = scrape_year(year)
            print(f"  {year}: {len(holidays)} holidays found")
            all_holidays.extend(holidays)
        except SystemExit:
            raise
        except Exception as e:
            print(f"  {year}: ERROR - {e}")
            if year == current_year:
                raise

    # Deduplicate by date+name
    seen = set()
    unique = []
    for h in all_holidays:
        key = (h["date"], h["name"])
        if key not in seen:
            seen.add(key)
            unique.append(h)

    unique.sort(key=lambda x: x["date"])
    write_yaml(unique, OUTPUT_PATH)
    print("Done.")


if __name__ == "__main__":
    main()
