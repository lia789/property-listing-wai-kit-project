from __future__ import annotations
import re
from typing import Optional, Tuple
from urllib.parse import urlparse, unquote







# --- Constants / small utilities ------------------------------------------------

# Sentinel for "Studio" bedrooms (kept to match your earlier scripts)
STUDIO_SENTINEL = 100

_NULL_STRS = {"", "na", "n/a", "none", "null", "nil", "nan", "-"}

_NUM_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")  # first numeric token (int/float),
_WS_RE = re.compile(r"\s+")


def _strip(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = s.strip()
    return s if s else None



# --- Field-specific cleaners ----------------------------------------------------

def split_area(area_state: Optional[str]) -> Optional[str]:
    s = _strip(area_state)
    if not s:
        return None
    return s.split(",")[0].strip() or None




def extract_list_id(url: Optional[str]) -> Optional[str]:

    if not url:
        return None
    # common "sale-<digits>" pattern
    m = re.search(r"(?:sale[-_/])(\d{6,})", url)
    if m:
        return m.group(1)
    # fallback: trailing digits in last path segment
    path = urlparse(url).path
    parts = [p for p in path.split("/") if p]
    if not parts:
        return None
    last = parts[-1]
    m2 = re.search(r"(\d{6,})$", last)
    return m2.group(1) if m2 else None




def get_condo_name(name_row: Optional[str]) -> Optional[str]:
    s = _strip(name_row)
    if not s:
        return None
    return s.split(",")[0].strip() or None




def extract_lat_long_from_url(u: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    try:
        if not u:
            return (None, None)
        q = unquote(u)

        m = re.search(r"markers=[^&]*?(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)", q)
        if not m:
            m = re.search(r"center=(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)", q)
        if not m:
            return (None, None)
        lat, lng = float(m.group(1)), float(m.group(2))
        return (lat, lng)
    except Exception:
        return (None, None)




def analyze_description(description: Optional[str]) -> dict:
    new_launch_keywords = [
        "new launch", "rebate", "direct developer", "early bird", "sale package",
        "new project", "free spa legal", "free legal", "free loan legal",
    ]
    auction_keywords = ["auction", "lelong", "reserve price", "bid", "bidding", "bidder", "bids"]
    urgent_sales_keywords = ["urgent", "must sell", "quick sale"]
    below_market_keywords = ["below market", "discount", "bargain", "fire sale"]

    text = (description or "").lower()

    def flag(keys):  # 0/1 integers as you used before
        return 1 if any(k in text for k in keys) else 0

    return {
        "new_project": flag(new_launch_keywords),
        "auction": flag(auction_keywords),
        "below_market_value": flag(below_market_keywords),
        "urgent": flag(urgent_sales_keywords),
    }





def clean_bedrooms(bed_rooms: Optional[str], studio_value: int = STUDIO_SENTINEL) -> Optional[int]:
    if bed_rooms is None:
        return None

    # primitives
    if isinstance(bed_rooms, (int, float)) and not isinstance(bed_rooms, bool):
        try:
            return int(bed_rooms)
        except Exception:
            return None

    s = _strip(str(bed_rooms))
    if not s:
        return None

    # take the first comma chunk as "bedrooms"
    first = s.split(",")[0].replace(" ", "").lower()

    if first.startswith("studio"):
        return studio_value

    # sum tokens split by '+', taking only pure digits
    parts = re.split(r"\+", first)
    nums = [int(p) for p in parts if p.isdigit()]
    if nums:
        return sum(nums)

    # fallback: first number anywhere
    m = _NUM_RE.search(first)
    return int(float(m.group())) if m else None




def clean_int_float(value_row: Optional[str]) -> Optional[float | int]:
    if value_row is None:
        return None

    s = str(value_row).replace("\xa0", " ").strip()
    if not s or s.lower() in _NULL_STRS:
        return None

    # Remove common separators so '1,800,000' becomes '1800000'
    s = s.replace(",", "")
    m = _NUM_RE.search(s)
    if not m:
        return None

    n = float(m.group())
    return int(n) if n.is_integer() else n





def normalize_whitespace(text: Optional[str]) -> Optional[str]:
    s = _strip(text)
    if not s:
        return None
    return _WS_RE.sub(" ", s)








def clean_posted_date(text):
    try:
        if text is None:
            return None
        s = str(text).strip()
        if not s:
            return None

        # match like "28 Sep 2025" or "5 September 2023" (optional trailing '.')
        m = re.search(r'(\d{1,2})\s+([A-Za-z]{3,9}\.?)\s+(\d{4})', s)
        if not m:
            return None

        day, mon_raw, year = m.groups()
        mon = mon_raw.rstrip('.') 
        mon = mon[:3].title()
        return f"{int(day)} {mon} {year}"
    except Exception:
        return None


def clean_tenure(tenure_row):
    try:
        if tenure_row is None:
            return None
        s = str(tenure_row).strip().strip("'\"")
        if not s:
            return None

        # remove 'tenure' with optional colon after it (e.g., "Tenure: Freehold")
        s = re.sub(r'(?i)\btenure\b\s*:?', '', s)

        # collapse extra spaces and trim quotes again
        s = re.sub(r'\s+', ' ', s).strip().strip("'\"")
        return s or None
    except Exception:
        return None
    

def clean_property_type(property_type_row):
    try:
        if property_type_row is None:
            return None
        s = str(property_type_row).strip().strip("'\"")
        if not s:
            return None

        # remove "for sale" (case-insensitive, allow extra spaces)
        s = re.sub(r'(?i)\bfor\s*sale\b', '', s)

        # normalize spaces & trim quotes again
        s = re.sub(r'\s+', ' ', s).strip().strip("'\"")
        return s or None
    except Exception:
        return None
    

def clean_property_title_type(property_title_type_row):
    """
    Removes the word 'title' (case-insensitive) from the string.
    Normalizes spaces and trims surrounding quotes.
    Returns None on empty/invalid input.
    """
    try:
        if property_title_type_row is None:
            return None
        s = str(property_title_type_row).strip().strip("'\"")
        if not s:
            return None

        # remove 'title' and any immediate trailing spaces (e.g., "Strata title" -> "Strata")
        s = re.sub(r'(?i)\btitle\b\s*', '', s)

        # collapse extra spaces & trim quotes again
        s = re.sub(r'\s+', ' ', s).strip().strip("'\"")
        return s or None
    except Exception:
        return None




def extract_lat_lng_from_script(json_text):
    try:
        if json_text is None:
            return (None, None)
        m = re.search(
            r'"lat"\s*:\s*(-?\d+(?:\.\d+)?)\s*,\s*"lng"\s*:\s*(-?\d+(?:\.\d+)?)',
            str(json_text),
            flags=re.DOTALL
        )
        if not m:
            return (None, None)
        return float(m.group(1)), float(m.group(2))
    except Exception:
        return (None, None)



import re
from datetime import datetime
from typing import Optional

_MONTH_MAP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

def _safe_date(y: int, m: int, d: int) -> Optional[str]:
    try:
        return datetime(y, m, d).strftime("%Y-%m-%d")
    except ValueError:
        return None

def clean_auction_date_iso(text: Optional[str], day_first: bool = True) -> Optional[str]:
    """
    Convert inputs like:
      'Auction on 23 Oct 2025' -> '2025-10-23'
      'auction on 2025-12-15 10:00' -> '2025-12-15'
      'Auction on 01/11/2025' -> '2025-11-01' (day_first=True)
    If parsing fails, return None.
    """
    if text is None:
        return None
    if not isinstance(text, str):
        text = str(text)

    s = text.strip()
    if not s:
        return None

    # remove 'Auction on' (case-insensitive), allow punctuation after it
    s = re.sub(r'^\s*auction\s*on\s*[:\-–—]?\s*', '', s, flags=re.IGNORECASE)

    # normalize: drop commas and ordinal suffixes (1st, 2nd, 3rd, 4th...)
    s = re.sub(r",", "", s)
    s = re.sub(r"(\d{1,2})(st|nd|rd|th)\b", r"\1", s, flags=re.IGNORECASE)

    # 1) ISO date present anywhere: YYYY-MM-DD
    m = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", s)
    if m:
        y, mo, d = map(int, m.groups())
        return _safe_date(y, mo, d)

    # 2) Day MonthName Year  (e.g., 23 Oct 2025, 7 September 2025, 3 Feb. 25)
    m = re.search(r"\b(\d{1,2})\s+([A-Za-z\.]+)\s+(\d{2,4})\b", s)
    if m:
        d, mon_txt, ytxt = m.groups()
        mon_key = mon_txt.strip(".").lower()
        if mon_key in _MONTH_MAP:
            day = int(d)
            year = int(ytxt)
            if year < 100:  # expand 2-digit year: 00-49 -> 2000s, 50-99 -> 1900s
                year = 2000 + year if year <= 49 else 1900 + year
            month = _MONTH_MAP[mon_key]
            return _safe_date(year, month, day)

    # 3) Numeric dates: dd/mm/yyyy, dd-mm-yyyy, mm/dd/yyyy (controlled by day_first)
    m = re.search(r"\b(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{2,4})\b", s)
    if m:
        a, b, ytxt = m.groups()
        year = int(ytxt)
        if year < 100:
            year = 2000 + year if year <= 49 else 1900 + year
        a = int(a); b = int(b)
        if day_first:
            day, month = a, b
        else:
            month, day = a, b
        return _safe_date(year, month, day)

    return None




# --- Module export surface -----------------------------------------------------

__all__ = [
    "STUDIO_SENTINEL",
    "split_area",
    "extract_list_id",
    "get_condo_name",
    "extract_lat_long_from_url",
    "analyze_description",
    "clean_bedrooms",
    "clean_int_float",
    "normalize_whitespace",
    "clean_posted_date",
    "clean_tenure",
    "clean_property_type",
    "clean_property_title_type",
    "extract_lat_lng_from_script",
    "clean_auction_date_iso",


]
