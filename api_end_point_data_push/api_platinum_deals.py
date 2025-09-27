# -*- coding: utf-8 -*-
# Daily sender: MySQL -> PropertyLab Platinum Deals API
# Validation, numeric coercion, URL check, per-table logging, retries, LIVE console counters
# NOTE: If price is missing/blank -> default to 0.0 (do NOT skip)

import os
import re
import time
import datetime
import requests
import pymysql
from dotenv import load_dotenv
from decimal import Decimal

# ── Load environment variables
load_dotenv()

# ── MySQL connection setup
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "property_listing")

# ── API endpoint and credentials
API_URL = "https://app.propertylab.tech/api/properties/platinum-deals/calculate-market-value"
PLATINUM_DEALS_API_KEY = os.getenv("PLATINUM_DEALS_API_KEY", "").strip()

# ── Simple sanity check for API key
if not PLATINUM_DEALS_API_KEY:
    raise RuntimeError("PLATINUM_DEALS_API_KEY is missing. Add it to your .env file.")

# ── Compute today's date (YYYY-MM-DD) for filtering
current_date = datetime.datetime.now().strftime('%Y-%m-%d')
# current_date = "2025-09-27"  # <- use for testing a fixed date





# ── Reset logs for a fresh daily run
with open("logs.txt", "w", encoding="utf-8") as f:
    f.write(f"=== Run started at {datetime.datetime.now()} (filter date: {current_date}) ===\n")

# ── Tiny logger
def log_status(message):
    with open("logs.txt", "a", encoding="utf-8") as log_file:
        log_file.write(f"{datetime.datetime.now()} - {message}\n")

# ── Clean "tenure": remove word 'tenure', keep only Freehold/Leasehold or None
def clean_property_tenure(value):
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in ("none", "null", "n/a"):
        return None
    s = re.sub(r'\btenure\b', '', s, flags=re.IGNORECASE).strip()
    if re.search(r'\bfree\s*hold\b|freehold', s, flags=re.IGNORECASE):
        return "Freehold"
    if re.search(r'\blease\s*hold\b|leasehold', s, flags=re.IGNORECASE):
        return "Leasehold"
    return None

# ── Normalize property_type to allowed set (default condo)
def normalize_property_type(value):
    if value is None:
        return "condo"
    s = str(value).strip().lower().replace(" ", "_").replace("-", "_")
    allowed = {"condo", "apartment", "serviced_residence", "flat", "landed"}
    return s if s in allowed else "condo"

# ── Helpers for validation / coercion / JSON
def is_blank(x):
    return x is None or (isinstance(x, str) and x.strip() == "")

from decimal import Decimal
def to_float_or_none(x):
    if x is None:
        return None
    if isinstance(x, Decimal):
        return float(x)
    try:
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

# NEW: price-specific coercion (missing/blank -> 0.0)
def to_float_or_zero(x):
    v = to_float_or_none(x)
    return 0.0 if v is None else v

def validate_payload(payload):
    """
    Returns (ok, missing, invalid).
    missing => required keys that are None/blank (0.0 is NOT blank)
    invalid => format/range issues (url format, lat/lon ranges)
    """
    required = ["property_name","listing_url","area","state","price","size",
                "property_type","longitude","latitude","type"]

    missing = [k for k in required if is_blank(payload.get(k))]
    invalid = []

    # listing_url must start with http or https
    url = payload.get("listing_url")
    if not is_blank(url) and not (url.startswith("http://") or url.startswith("https://")):
        invalid.append("listing_url_format")

    # lat/lon range checks (only if present)
    lat = payload.get("latitude")
    lon = payload.get("longitude")
    if lat is not None and not (-90 <= lat <= 90):
        invalid.append("latitude_range")
    if lon is not None and not (-180 <= lon <= 180):
        invalid.append("longitude_range")

    return (len(missing) == 0 and len(invalid) == 0, missing, invalid)

def to_jsonable(x):
    if isinstance(x, Decimal):
        return float(x)
    if isinstance(x, dict):
        return {k: to_jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [to_jsonable(v) for v in x]
    return x

# ── POST with retries; include table + list_id in logs, and print live
def send_api_request(payload, table_label, list_id, name):
    headers = {
        "Api-Key": PLATINUM_DEALS_API_KEY,
        "Content-Type": "application/json"
    }
    attempt = 1
    while attempt <= 3:
        try:
            response = requests.post(
                API_URL,
                headers=headers,
                json=to_jsonable(payload),
                timeout=30
            )
            if 200 <= response.status_code < 300:
                log_status(f"[{table_label}] list_id={list_id} name='{name}' API success (status {response.status_code})")
                print(f"[{table_label}] list_id={list_id} name='{name}' -> SUCCESS ({response.status_code})", flush=True)
                return True
            else:
                log_status(f"[{table_label}] list_id={list_id} name='{name}' API failed "
                           f"(status {response.status_code}, body: {response.text.strip()[:500]})")
                print(f"[{table_label}] list_id={list_id} name='{name}' -> FAIL attempt {attempt} ({response.status_code})", flush=True)
        except Exception as e:
            log_status(f"[{table_label}] list_id={list_id} name='{name}' API error ({type(e).__name__}: {e})")
            print(f"[{table_label}] list_id={list_id} name='{name}' -> ERROR attempt {attempt} ({type(e).__name__})", flush=True)
        attempt += 1
        time.sleep(0.001)  # backoff between retries
    return False

# ── Connect to MySQL (DictCursor -> access by column name)
connection = pymysql.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DB,
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=False,
    charset="utf8mb4"
)

try:
    cursor = connection.cursor()

    # ── Filter by DATE(data_scraping_date) for today's rows; skip already-updated
    iproperty_sql = """
        SELECT
            list_id, name, url, area, state, price, bed_rooms, built_up_size,
            posted_date, tenure, property_type, lat, lng
        FROM `iproperty-new-listing`
        WHERE DATE(data_scraping_date) = %s
          AND (api_update_status IS NULL OR api_update_status = 0)
    """
    property_guru_sql = """
        SELECT
            list_id, name, url, area, state, price, bed_rooms, built_up_size,
            posted_date, tenure, property_type, lat, lng
        FROM `property-guru-new-listing`
        WHERE DATE(data_scraping_date) = %s
          AND (api_update_status IS NULL OR api_update_status = 0)
    """

    cursor.execute(iproperty_sql, (current_date,))
    iproperty_rows = cursor.fetchall()

    cursor.execute(property_guru_sql, (current_date,))
    property_guru_rows = cursor.fetchall()

    # Console + log summary of fetched
    print(f"Fetched: iproperty={len(iproperty_rows)}, prop-guru={len(property_guru_rows)} for {current_date}", flush=True)
    log_status(f"Fetched {len(iproperty_rows)} rows from iproperty-new-listing for {current_date}.")
    log_status(f"Fetched {len(property_guru_rows)} rows from property-guru-new-listing for {current_date}.")

    # LIVE counters
    to_send = 0       # valid rows (will be sent)
    success = 0       # successful POSTs
    fail = 0          # exhausted retries (non-2xx)
    skipped = 0       # skipped due to missing/invalid before POST

    per_request_delay = 0.4  # pacing so we don't hammer API/DB

    # ── Process iproperty-new-listing rows
    for row in iproperty_rows:
        table_label = "iproperty"
        list_id = row["list_id"]
        name = (row["name"] or "").strip()

        payload = {
            "property_name":   name,
            "listing_url":     (row["url"] or "").strip(),
            "area":            (row["area"] or "").strip(),
            "state":           (row["state"] or "").strip(),
            "price":           to_float_or_zero(row["price"]),      # <-- default 0.0 if missing/blank
            "no_of_bedroom":   to_float_or_none(row["bed_rooms"]),
            "no_of_bathroom":  None,  # not in DB
            "no_of_carpark":   None,  # not in DB
            "size":            to_float_or_none(row["built_up_size"]),
            "property_tenure": clean_property_tenure(row["tenure"]),
            "property_type":   normalize_property_type(row["property_type"]),
            # STANDARD mapping: lng -> longitude, lat -> latitude
            "longitude":       to_float_or_none(row["lng"]),
            "latitude":        to_float_or_none(row["lat"]),
            "type":            "subsale"
        }

        ok, missing, invalid = validate_payload(payload)
        if not ok:
            skipped += 1
            msg = f"[{table_label}] list_id={list_id} name='{name}' SKIPPED missing={missing} invalid={invalid}"
            log_status(msg)
            print(msg, flush=True)
            print(f"Live => to_send:{to_send} success:{success} fail:{fail} skipped:{skipped}", flush=True)
            continue

        to_send += 1
        ok_post = send_api_request(payload, table_label, list_id, name)
        if ok_post:
            success += 1
            cursor.execute(
                "UPDATE `iproperty-new-listing` SET api_update_status = 1 WHERE list_id = %s",
                (list_id,)
            )
            connection.commit()
        else:
            fail += 1
            log_status(f"[{table_label}] list_id={list_id} name='{name}' Giving up after retries")

        print(f"Live => to_send:{to_send} success:{success} fail:{fail} skipped:{skipped}", flush=True)
        time.sleep(per_request_delay)

    # ── Process property-guru-new-listing rows
    for row in property_guru_rows:
        table_label = "prop-guru"
        list_id = row["list_id"]
        name = (row["name"] or "").strip()

        payload = {
            "property_name":   name,
            "listing_url":     (row["url"] or "").strip(),
            "area":            (row["area"] or "").strip(),
            "state":           (row["state"] or "").strip(),
            "price":           to_float_or_zero(row["price"]),      # <-- default 0.0 if missing/blank
            "no_of_bedroom":   to_float_or_none(row["bed_rooms"]),
            "no_of_bathroom":  None,
            "no_of_carpark":   None,
            "size":            to_float_or_none(row["built_up_size"]),
            "property_tenure": clean_property_tenure(row["tenure"]),
            "property_type":   normalize_property_type(row["property_type"]),
            # STANDARD mapping: lng -> longitude, lat -> latitude
            "longitude":       to_float_or_none(row["lng"]),
            "latitude":        to_float_or_none(row["lat"]),
            "type":            "subsale"
        }

        ok, missing, invalid = validate_payload(payload)
        if not ok:
            skipped += 1
            msg = f"[{table_label}] list_id={list_id} name='{name}' SKIPPED missing={missing} invalid={invalid}"
            log_status(msg)
            print(msg, flush=True)
            print(f"Live => to_send:{to_send} success:{success} fail:{fail} skipped:{skipped}", flush=True)
            continue

        to_send += 1
        ok_post = send_api_request(payload, table_label, list_id, name)
        if ok_post:
            success += 1
            cursor.execute(
                "UPDATE `property-guru-new-listing` SET api_update_status = 1 WHERE list_id = %s",
                (list_id,)
            )
            connection.commit()
        else:
            fail += 1
            log_status(f"[{table_label}] list_id={list_id} name='{name}' Giving up after retries")

        print(f"Live => to_send:{to_send} success:{success} fail:{fail} skipped:{skipped}", flush=True)
        time.sleep(per_request_delay)

    # Final console summary
    print("="*60, flush=True)
    print(f"SUMMARY for {current_date}", flush=True)
    print(f"  Valid (to_send): {to_send}", flush=True)
    print(f"  Success:         {success}", flush=True)
    print(f"  Fail:            {fail}", flush=True)
    print(f"  Skipped:         {skipped}", flush=True)
    print("="*60, flush=True)

    log_status("Run completed.")

except Exception as e:
    log_status(f"FATAL ERROR: {type(e).__name__}: {e}")
    print(f"\nFATAL ERROR: {type(e).__name__}: {e}", flush=True)
    try:
        connection.rollback()
    except Exception:
        pass
finally:
    try:
        connection.close()
    except Exception:
        pass
