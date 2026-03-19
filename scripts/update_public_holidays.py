"""
update_public_holidays.py

Scrapes public holiday dates for Mauritius from officeholidays.com
and writes them to data/public_holidays.yaml.

Run via GitHub Actions on the 1st of each month, or manually:
  python scripts/update_public_holidays.py
"""

import requests
import yaml
import re
import sys
from datetime import datetime, timezone
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

OUTPUT_PATH = "data/public_holidays.yaml"

# Month name to number mapping
MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}


def scrape_year(year: int) -> list:
    url = f"https://www.officeholidays.com/countries/mauritius/{year}"
    print(f"Fetching: {url}")
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    holidays = []

    # officeholidays.com renders holidays in a table with class "country-table"
    table = soup.find("table", class_=re.compile(r"country", re.I))
    if not table:
        # Fallback: look for any table with date-like content
        table = soup.find("table")

    if table:
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            date_text = cells[0].get_text(strip=True)
            name_text = cells[1].get_text(strip=True)

            if not date_text or not name_text:
                continue

            # Parse dates like "Jan 1", "Mar 12", "Dec 25"
            match = re.match(r"(\w{3})\s+(\d{1,2})", date_text)
            if match:
                month_str, day_str = match.group(1), match.group(2)
                month = MONTHS.get(month_str[:3].title())
                if month:
                    try:
                        date_iso = f"{year}-{month:02d}-{int(day_str):02d}"
                        holidays.append({
                            "date": date_iso,
                            "name": name_text
                        })
                    except ValueError:
                        continue

    if not holidays:
        print("WARNING: No holidays found in table. The page structure may have changed.")
        print("Manual update required — check https://publicholidays.mu/")
        sys.exit(1)

    # Sort by date
    holidays.sort(key=lambda x: x["date"])
    return holidays


def write_yaml(year: int, holidays: list, path: str):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    content = {
        "__comment__": [
            "Mauritius public holidays",
            "Source: publicholidays.mu (via officeholidays.com)",
            "Updated automatically on the 1st of each month.",
            "To update manually: Actions → Update public holidays → Run workflow",
            f"Last updated: {now}"
        ],
        "year": year,
        "holidays": holidays
    }

    # Write with a hand-crafted header comment since PyYAML doesn't support comments
    with open(path, "w") as f:
        f.write(f"# Mauritius public holidays\n")
        f.write(f"# Source: publicholidays.mu (via officeholidays.com)\n")
        f.write(f"# Updated automatically on the 1st of each month.\n")
        f.write(f"# To update manually: Actions \u2192 Update public holidays \u2192 Run workflow\n")
        f.write(f"# Last updated: {now}\n\n")
        f.write(f"year: {year}\n")
        f.write("holidays:\n")
        for h in holidays:
            f.write(f'  - date: "{h["date"]}"\n')
            f.write(f'    name: "{h["name"]}"\n')

    print(f"Written {len(holidays)} holidays to {path}")


def main():
    # Scrape current year and next year so we always have upcoming holidays
    today = datetime.now()
    current_year = today.year
    next_year = current_year + 1

    all_holidays = []

    for year in [current_year, next_year]:
        try:
            holidays = scrape_year(year)
            print(f"  {year}: {len(holidays)} holidays found")
            all_holidays.extend(holidays)
        except Exception as e:
            print(f"  {year}: ERROR - {e}")
            if year == current_year:
                # Current year is mandatory
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

    write_yaml(current_year, unique, OUTPUT_PATH)
    print("Done.")


if __name__ == "__main__":
    main()
