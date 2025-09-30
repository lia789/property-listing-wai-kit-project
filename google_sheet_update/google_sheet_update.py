# google_sheet_update.py

import os
import sys
import logging
from decimal import Decimal
from datetime import datetime, date
from zoneinfo import ZoneInfo

import pymysql
from dotenv import load_dotenv, find_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# =========================
# Logging
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("sheet-update")


# =========================
# Load .env robustly (systemd-safe)
# =========================
dotenv_path = find_dotenv()
if not dotenv_path:
    # fallback to local directory (where this script resides)
    dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path, override=False)
log.info(f".env loaded from: {dotenv_path if os.path.exists(dotenv_path) else 'not found (using environment)'}")


# =========================
# Config
# =========================
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "property_listing")

SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SA_FILE", "property-listing-wai-kit.json")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1tg")  # <-- make sure this is the full ID

# MySQL tables -> Google Sheet tab names (must match existing sheet tabs)
TABLES = {
    "iproperty-auction-listing": "iproperty-auction-listing",
    "iproperty-new-listing": "iproperty-new-listing",
    "property-guru-new-listing": "property-guru-new-listing",
}

# Column in MySQL that stores the scraping timestamp/date
DATE_COLUMN = "data_scraping_date"  # uses DATE() on this column in the WHERE clause

# Timezone-aware "today" (Asia/Dhaka)
dhaka_today = datetime.now(ZoneInfo("Asia/Dhaka")).date()
dhaka_today_str = dhaka_today.strftime("%Y-%m-%d")

# dhaka_today_str = "2025-09-28"



log.info(f"Using Asia/Dhaka current date: {dhaka_today_str}")



# =========================
# Helpers
# =========================
def normalize_value(v):
    """Convert DB values into safe types for Sheets API."""
    if v is None:
        return ""
    if isinstance(v, Decimal):
        # You can also return str(v) if you want exact precision; float is usually fine for Sheets.
        return float(v)
    if isinstance(v, datetime):
        # Normalize any timezone-naive datetimes as plain string
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.strftime("%Y-%m-%d")
    # bool, int, float, str are fine
    return v


def col_index_to_letter(idx_zero_based: int) -> str:
    """0 -> A, 1 -> B, ..., 25 -> Z, 26 -> AA, etc."""
    idx = idx_zero_based
    letters = []
    while idx >= 0:
        idx, rem = divmod(idx, 26)
        letters.append(chr(ord('A') + rem))
        idx -= 1
    return ''.join(reversed(letters))


def normalize_header_name(h: str) -> str:
    """Lowercase, trim, and underscore header names for matching."""
    return h.strip().lower().replace(" ", "_")


def get_sheet_headers(svc, sheet_name: str):
    """Fetch header row (row 1). Return list of header strings or [] if missing."""
    res = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{sheet_name}!1:1"
    ).execute()
    values = res.get("values", [])
    headers = values[0] if values else []
    return headers


def find_list_id_col_index(headers: list) -> int:
    """Find the column index of 'list_id' (case-insensitive, space-insensitive). Default to 0 if not found."""
    norm_headers = [normalize_header_name(h) for h in headers]
    try:
        return norm_headers.index("list_id")
    except ValueError:
        return 0  # fallback to first column if not found


def get_existing_ids(svc, sheet_name: str, list_id_col_idx: int) -> set:
    """Read entire list_id column (skip header) and return a set of string IDs."""
    col_letter = col_index_to_letter(list_id_col_idx)
    rng = f"{sheet_name}!{col_letter}:{col_letter}"
    res = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=rng
    ).execute()
    rows = res.get("values", [])
    if not rows:
        return set()

    # Skip header (row 1). Subsequent rows may be shorter lists (Sheets API quirk)
    existing = set()
    for i, row in enumerate(rows, start=1):
        if i == 1:
            continue  # header
        if not row:
            continue
        cell = row[0]
        if cell is None:
            continue
        s = str(cell).strip()
        if s:
            existing.add(s)
    return existing


def row_to_sheet_values(row: dict, headers: list) -> list:
    """Map a DB row dict into a list matching the sheet's header order."""
    values = []
    for h in headers:
        key = h  # assume DB keys match sheet header names
        # Try exact header key first
        v = row.get(key)
        if v is None:
            # Try normalized matching (very light heuristic)
            # e.g., 'list_id' header vs 'list_id' key is fine; or 'List ID' header vs 'list_id' key
            norm_key = normalize_header_name(key)
            # Try a direct hit on normalized keys of the dict
            v = next((row[k] for k in row.keys() if normalize_header_name(k) == norm_key), None)
        values.append(normalize_value(v))
    return values


# =========================
# Main
# =========================
def main():
    # Connect to MySQL
    try:
        connection = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            port=MYSQL_PORT,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )
        log.info(f"Connected to MySQL {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")
    except Exception as e:
        log.exception("Failed to connect to MySQL")
        sys.exit(1)

    # Auth Google Sheets
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        service = build("sheets", "v4", credentials=credentials)
        log.info("Authenticated with Google Sheets API.")
    except Exception as e:
        log.exception("Failed to authenticate with Google Sheets API")
        sys.exit(1)

    # Process each table/sheet
    for table, sheet_name in TABLES.items():
        log.info(f"--- Processing table '{table}' -> sheet '{sheet_name}' ---")

        # Fetch sheet headers
        try:
            headers = get_sheet_headers(service, sheet_name)
            if not headers:
                log.warning(f"Sheet '{sheet_name}' has no header row (row 1). Skipping.")
                continue
            log.info(f"Sheet '{sheet_name}' headers: {headers}")
        except HttpError as e:
            log.exception(f"Error reading headers from sheet '{sheet_name}'")
            continue

        # Find list_id column from headers
        list_id_col_idx = find_list_id_col_index(headers)
        log.info(f"Detected 'list_id' column at index {list_id_col_idx} (A=0).")

        # Read existing IDs for dedupe
        try:
            existing_ids = get_existing_ids(service, sheet_name, list_id_col_idx)
            log.info(f"Existing IDs on sheet '{sheet_name}': {len(existing_ids)}")
        except HttpError:
            log.exception(f"Failed to read existing list IDs from sheet '{sheet_name}'.")
            continue

        # Query rows for Dhaka 'today' using DATE() filter
        rows = []
        q = f"""
            SELECT *
            FROM `{table}`
            WHERE DATE(`{DATE_COLUMN}`) = %s
        """
        try:
            with connection.cursor() as cur:
                cur.execute(q, (dhaka_today_str,))
                rows = cur.fetchall()
            log.info(f"MySQL rows fetched for {dhaka_today_str}: {len(rows)}")
        except Exception:
            log.exception(f"Query failed for table `{table}` on date {dhaka_today_str}. "
                          f"Check column `{DATE_COLUMN}` exists and types are correct.")
            continue

        if not rows:
            log.warning(f"No DB rows found for {dhaka_today_str} in `{table}`. "
                        f"(Timezone? Column `{DATE_COLUMN}`?)")
            continue

        # Filter new (not already on sheet)
        # We expect 'list_id' key in the DB row dict. We'll compare as strings.
        new_rows = []
        missing_list_id_count = 0
        for r in rows:
            if "list_id" not in r:
                missing_list_id_count += 1
                continue
            lid = str(r["list_id"]).strip()
            if lid and lid not in existing_ids:
                new_rows.append(r)

        log.info(f"Rows with missing 'list_id': {missing_list_id_count}")
        log.info(f"New rows to append (deduped): {len(new_rows)}")

        if not new_rows:
            log.info(f"No new data to add to {sheet_name}.")
            continue

        # Map rows into sheet's header order and convert values
        values_matrix = [row_to_sheet_values(r, headers) for r in new_rows]

        # Append
        try:
            body = {"values": values_matrix}
            result = service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{sheet_name}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body=body,
            ).execute()
            updates = result.get("updates", {})
            updated_rows = updates.get("updatedRows", 0)
            log.info(f"Appended {updated_rows} rows to '{sheet_name}'.")
        except HttpError:
            log.exception(f"Failed to append to sheet '{sheet_name}'.")
            continue

    # Close DB
    try:
        connection.close()
        log.info("MySQL connection closed.")
    except Exception:
        log.warning("Failed to close MySQL connection cleanly.")


if __name__ == "__main__":
    main()
