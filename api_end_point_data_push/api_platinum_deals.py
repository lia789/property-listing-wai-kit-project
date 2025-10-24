import os
import re
import time
import datetime
import requests
import pymysql
from dotenv import load_dotenv
from decimal import Decimal
from data_clean import clean_property_tenure, clean_posted_date, normalize_property_type, is_blank, to_float_or_none, to_float_or_zero, to_jsonable, clean_bed_rooms, auction_date_clean
import json



# Environment variable
load_dotenv()

# Setup
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "property_listing")
API_URL = "https://app.propertylab.tech/api/properties/platinum-deals/calculate-market-value"
PLATINUM_DEALS_API_KEY = os.getenv("PLATINUM_DEALS_API_KEY", "").strip()
REQUEST_DELAY = 0.5
DEBUG_LOG_BODY_MAX = 4000  # keep logs readable


# Log code
with open("logs.txt", "w", encoding="utf-8") as f:
    f.write(f"=== Run started at {datetime.datetime.now()} (NO DATE FILTER) ===\n")

def log_status(message):
    with open("logs.txt", "a", encoding="utf-8") as log_file:
        log_file.write(f"{datetime.datetime.now()} - {message}\n")








def validate_payload(payload):
    required = [
        "property_name",
        "listing_url",
        "area",
        "state",
        "price",
        "no_of_bedroom",
        "size",
        "property_type",
        "longitude",
        "latitude",
        "type",
        ]


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





def precheck_payload(payload):
    """
    Return a list of human-readable issues so we can SKIP rows
    that will obviously fail the API (range/type rules, empties, etc.)
    """
    issues = []

    # URL format
    url = payload.get("listing_url")
    if not url or not (url.startswith("http://") or url.startswith("https://")):
        issues.append("listing_url must start with http/https")

    # Bedrooms: API error you saw earlier says 1..5
    br = payload.get("no_of_bedroom")
    if br is None:
        issues.append("no_of_bedroom is missing")
    else:
        try:
            ibr = int(br)
            if not (1 <= ibr <= 5):
                issues.append("no_of_bedroom must be 1..5")
        except Exception:
            issues.append("no_of_bedroom must be integer")

    # Size should be positive if provided
    sz = payload.get("size")
    if sz is None or (isinstance(sz, (int, float)) and sz <= 0):
        issues.append("size must be a positive number")

    # Price should be > 0 (adjust if your API allows 0)
    price = payload.get("price")
    if price is None or (isinstance(price, (int, float)) and price <= 0):
        issues.append("price must be > 0")

    # Lat/Lng required and in range
    lat = payload.get("latitude")
    lng = payload.get("longitude")
    if lat is None or lng is None:
        issues.append("latitude/longitude required")
    else:
        if not (-90 <= lat <= 90):
            issues.append("latitude out of range (-90..90)")
        if not (-180 <= lng <= 180):
            issues.append("longitude out of range (-180..180)")

    # Property type limited set (adjust to real API allowlist)
    allowed_pt = {"condo", "apartment", "serviced_residence", "flat", "landed"}
    if payload.get("property_type") not in allowed_pt:
        issues.append(f"property_type must be one of {sorted(allowed_pt)}")

    # listing_date must be ISO string
    if not isinstance(payload.get("listing_date"), str):
        issues.append("listing_date must be ISO date string (YYYY-MM-DD)")

    return issues














def _payload_snapshot(payload: dict) -> str:
    """One-line ordered preview of the payload for the logs."""
    keys_order = [
        "property_name","listing_url","listing_date","area","state","price",
        "no_of_bedroom","no_of_bathroom","no_of_carpark","size",
        "property_tenure","property_type","longitude","latitude","type","auction_date"
    ]
    show = {k: payload.get(k) for k in keys_order if k in payload}
    # shorten long strings
    for k,v in list(show.items()):
        if isinstance(v, str) and len(v) > 200:
            show[k] = v[:200] + "…"
    try:
        return json.dumps(show, ensure_ascii=False)
    except Exception:
        return str(show)

def _flatten_api_errors(body_text: str):
    """Try to parse common API error shapes to a compact list of reasons."""
    try:
        data = json.loads(body_text)
    except Exception:
        return [body_text.strip()]

    reasons = []
    # Common shapes: {"message": "...", "errors": {"field": ["msg", ...]}}
    if isinstance(data, dict):
        msg = data.get("message")
        if msg:
            reasons.append(str(msg))
        errs = data.get("errors") or data.get("error") or {}
        if isinstance(errs, dict):
            for field, msgs in errs.items():
                if isinstance(msgs, (list, tuple)):
                    reasons.append(f"{field}: " + " | ".join(str(m) for m in msgs))
                else:
                    reasons.append(f"{field}: {msgs}")
    if not reasons:
        reasons.append(str(data))
    return reasons



def send_api_request(payload, table_label, list_id, name):
    headers = {
        "Api-Key": PLATINUM_DEALS_API_KEY,
        "Content-Type": "application/json"
    }
    attempt = 1
    while attempt <= 2:
        time.sleep(0.5)
        try:
            response = requests.post(
                API_URL,
                headers=headers,
                json=to_jsonable(payload),
                timeout=30
            )
            status = response.status_code
            if 200 <= status < 300:
                log_status(f"[{table_label}] list_id={list_id} name='{name}' API success ({status})")
                print(f"[{table_label}] list_id={list_id} name='{name}' -> SUCCESS ({status})", flush=True)
                return True
            else:
                body = (response.text or "")[:DEBUG_LOG_BODY_MAX]
                reasons = _flatten_api_errors(body)
                snap = _payload_snapshot(payload)
                log_status(
                    f"[{table_label}] list_id={list_id} name='{name}' API failed "
                    f"status={status} reasons={reasons} payload={snap}"
                )
                # Console: keep it short but informative
                first_reason = reasons[0] if reasons else f"HTTP {status}"
                print(
                    f"[{table_label}] list_id={list_id} name='{name}' -> FAIL attempt {attempt} ({status}) "
                    f"reason: {first_reason}",
                    flush=True
                )
        except Exception as e:
            snap = _payload_snapshot(payload)
            log_status(f"[{table_label}] list_id={list_id} name='{name}' API error ({type(e).__name__}: {e}) payload={snap}")
            print(f"[{table_label}] list_id={list_id} name='{name}' -> ERROR attempt {attempt} ({type(e).__name__})", flush=True)
        attempt += 1
    return False














# Connect to MySQL
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



    ### Iproperty new listing

    # NO DATE FILTER: only check api_update_status
    iproperty_sql = """
        SELECT
            list_id, name, url, area, state, price, bed_rooms, bath, built_up_size,
            posted_date, tenure, property_type, lat, lng, parking
        FROM `iproperty-new-listing`
        WHERE (api_update_status IS NULL OR api_update_status = 0)
        ORDER BY list_id ASC
    """

    # Cursor iproperty new listing
    cursor.execute(iproperty_sql)
    iproperty_rows = cursor.fetchall()

    # Console + log summary of fetched
    log_status(f"Fetched {len(iproperty_rows)} rows from iproperty-new-listing (no date filter).")





    ### Property Guru new listing

    # NO DATE FILTER: only check api_update_status
    property_guru_sql = """
        SELECT
            list_id, name, url, area, state, price, bed_rooms,bath, built_up_size,
            posted_date, tenure, property_type, lat, lng
        FROM `property-guru-new-listing`
        WHERE (api_update_status IS NULL OR api_update_status = 0)
        ORDER BY list_id ASC
    """

    # Cursor property guru new listing
    cursor.execute(property_guru_sql)
    property_guru_rows = cursor.fetchall()

    # Console + log summary of fetched
    log_status(f"Fetched {len(property_guru_rows)} rows from property-guru-new-listing (no date filter).")




    ### Iproperty auction listing

    # NO DATE FILTER: only check api_update_status
    iproperty_auction_sql = """
        SELECT
            list_id, name, url, area, state, price, bed_rooms,bath, built_up_size,
            posted_date, tenure, property_type, lat, lng, auction_date, parking
        FROM `iproperty-auction-listing`
        WHERE (api_update_status IS NULL OR api_update_status = 0)
        ORDER BY list_id ASC
    """

    # Cursor iproperty auction listing
    cursor.execute(iproperty_auction_sql)
    iproperty_auction_rows = cursor.fetchall()

    # Console + log summary of fetched
    log_status(f"Fetched {len(iproperty_auction_rows)} rows from iproperty-auction-listing (no date filter).")





    








    # Live counter for monitoring
    to_send = 0       # valid rows (will be sent)
    success = 0       # successful POSTs
    fail = 0          # exhausted retries (non-2xx)
    skipped = 0       # skipped due to missing/invalid before POST

    per_request_delay = REQUEST_DELAY






    # Process iproperty-new-listing rows
    for row in iproperty_rows:
        table_label = "iproperty"
        list_id = row["list_id"]
        name = (row["name"] or "").strip()

        payload = {
            "property_name":   name,
            "listing_url":     (row["url"] or "").strip(),
            "listing_date":    clean_posted_date(row["posted_date"]),
            "area":            (row["area"] or "").strip(),
            "state":           (row["state"] or "").strip(),
            "price":           to_float_or_zero(row["price"]),      # default 0.0 if missing/blank
            "no_of_bedroom":   clean_bed_rooms(row["bed_rooms"]),
            "no_of_bathroom":  to_float_or_none(row["bath"]),
            "no_of_carpark":   row["parking"],  # Add them too
            "size":            to_float_or_none(row["built_up_size"]),
            "property_tenure": clean_property_tenure(row["tenure"]),
            "property_type":   normalize_property_type(row["property_type"]),
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





    # Process property-guru-new-listing rows
    for row in property_guru_rows:
        table_label = "prop-guru"
        list_id = row["list_id"]
        name = (row["name"] or "").strip()

        payload = {
            "property_name":   name,
            "listing_url":     (row["url"] or "").strip(),
            "area":            (row["area"] or "").strip(),
            "listing_date":    clean_posted_date(row["posted_date"]),
            "state":           (row["state"] or "").strip(),
            "price":           to_float_or_zero(row["price"]),      # default 0.0 if missing/blank
            "no_of_bedroom":   clean_bed_rooms(row["bed_rooms"]),
            "no_of_bathroom":  to_float_or_none(row["bath"]),
            "no_of_carpark":   None,
            "size":            to_float_or_none(row["built_up_size"]),
            "property_tenure": clean_property_tenure(row["tenure"]),
            "property_type":   normalize_property_type(row["property_type"]),
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





    # Process iproperty-auction-listing rows
    for row in iproperty_auction_rows:
        table_label = "iproperty-auction"
        list_id = row["list_id"]
        name = (row["name"] or "").strip()

        payload = {
            "property_name":   name,
            "listing_url":     (row["url"] or "").strip(),
            "listing_date":    clean_posted_date(row["posted_date"]),
            "auction_date":    auction_date_clean(row["auction_date"]),
            "area":            (row["area"] or "").strip(),
            "state":           (row["state"] or "").strip(),
            "price":           to_float_or_zero(row["price"]),
            "no_of_bedroom":   clean_bed_rooms(row["bed_rooms"]),
            "no_of_bathroom":  to_float_or_none(row["bath"]),
            "no_of_carpark":   row["parking"],
            "size":            to_float_or_none(row["built_up_size"]),
            "property_tenure": clean_property_tenure(row["tenure"]),
            "property_type":   normalize_property_type(row["property_type"]),
            "longitude":       to_float_or_none(row["lng"]),
            "latitude":        to_float_or_none(row["lat"]),
            "type":            "auction"
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
                "UPDATE `iproperty-auction-listing` SET api_update_status = 1 WHERE list_id = %s",
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
    print("SUMMARY (no date filter)", flush=True)
    print(f"  Valid (to_send): {to_send}", flush=True)
    print(f"  Success:         {success}", flush=True)
    print(f"  Fail:            {fail}", flush=True)
    print(f"  Skipped:         {skipped}", flush=True)
    print("="*60, flush=True)

    log_status("Run completed.")









# Cleanup finally block with error handling
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
