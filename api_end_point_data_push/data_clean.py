import re
import datetime
from decimal import Decimal


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




def clean_posted_date(text) -> str:
    """Convert '24 Sep 2025' → '2025-09-24'. If it fails, return today's date."""
    s = str(text).strip()
    s = re.sub(r",", "", s)
    s = re.sub(r"(\d{1,2})(st|nd|rd|th)\b", r"\1", s, flags=re.I)
    s = re.sub(r"[-/]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\bsept\b", "Sep", s, flags=re.I).title()
    for fmt in ("%d %b %Y", "%d %B %Y"):
        try:
            return datetime.datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass

    # Fallback: today's date
    return datetime.datetime.today().date().isoformat()




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





# price-specific coercion (missing/blank -> 0.0)
def to_float_or_zero(x):
    v = to_float_or_none(x)
    return 0.0 if v is None else v




def to_jsonable(x):
    if isinstance(x, Decimal):
        return float(x)
    if isinstance(x, dict):
        return {k: to_jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [to_jsonable(v) for v in x]
    return x







import re
from decimal import Decimal

def clean_bed_rooms(value, *, clamp=False):
    """
    Return bedrooms as an int in [1..5]. If it's 100 -> 1.
    If clamp=True, values >5 become 5 (else they return None).
    """
    if value is None:
        return None

    n = None

    # direct numeric types
    if isinstance(value, (int, float, Decimal)):
        try:
            n = int(float(value))
        except Exception:
            return None
    else:
        s = str(value).strip()
        if not s:
            return None

        # handle "3+1" etc.
        if "+" in s:
            nums = re.findall(r"\d+", s)
            if nums:
                try:
                    n = sum(int(x) for x in nums)
                except Exception:
                    n = None
        else:
            m = re.search(r"\d+", s)
            if m:
                try:
                    n = int(m.group())
                except Exception:
                    n = None

    if n is None:
        return None

    # special case
    if n == 100:
        return 100

    if clamp:
        if n < 1:
            return None
        return 5 if n > 5 else n  # clamp to API max
    else:
        return n if 1 <= n <= 5 else None





import re
import datetime as _dt

def auction_date_clean(value):
    """
    Normalize various date inputs to 'YYYY-MM-DD' (string).
    If it can't be parsed, return None.

    Accepts: str, datetime.date, datetime.datetime

    Examples:
      "2025-10-27" -> "2025-10-27"
      "27 Oct 2025" -> "2025-10-27"
      "Auction on 23 Oct 2025" -> "2025-10-23"
      _dt.date(2025,10,16) -> "2025-10-16"
      "0000-00-00" -> None
      "" or None -> None
    """
    if value is None:
        return None

    # Direct date/datetime
    if isinstance(value, (_dt.date, _dt.datetime)):
        return (value if isinstance(value, _dt.date) and not isinstance(value, _dt.datetime) else value.date()).isoformat()

    s = str(value).strip()
    if not s:
        return None

    # Common "null-ish" bad dates
    if s in {"0000-00-00", "0000/00/00", "0000.00.00"}:
        return None

    # First, try to extract a date-like substring from messy text
    # 1) ISO-ish: 2025-10-27 / 2025/10/27 / 2025.10.27
    m = re.search(r"\b(\d{4}[-/.]\d{1,2}[-/.]\d{1,2})\b", s)
    if m:
        part = m.group(1).replace(".", "-").replace("/", "-")
        try:
            dt = _dt.datetime.strptime(part, "%Y-%m-%d").date()
            return dt.isoformat()
        except ValueError:
            # fall through to other formats
            pass

    # Normalize ordinal suffixes, commas, multiple spaces; fix "sept" → "Sep"
    t = re.sub(r"(\d{1,2})(st|nd|rd|th)\b", r"\1", s, flags=re.I)
    t = re.sub(r",", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"\bsept\b", "Sep", t, flags=re.I)

    # 2) Day first with named month: "27 Oct 2025" / "27 October 2025"
    m = re.search(r"\b(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})\b", t)
    if m:
        cand = m.group(1).title()
        for fmt in ("%d %b %Y", "%d %B %Y"):
            try:
                dt = _dt.datetime.strptime(cand, fmt).date()
                return dt.isoformat()
            except ValueError:
                pass

    # 3) Month first with named month: "Oct 27 2025" / "October 27 2025"
    m = re.search(r"\b([A-Za-z]{3,9}\s+\d{1,2}\s+\d{4})\b", t)
    if m:
        cand = m.group(1).title()
        for fmt in ("%b %d %Y", "%B %d %Y"):
            try:
                dt = _dt.datetime.strptime(cand, fmt).date()
                return dt.isoformat()
            except ValueError:
                pass

    # 4) Numeric D/M/Y or M/D/Y with dashes or slashes
    # Prefer D/M/Y (common outside US) before M/D/Y
    num = re.search(r"\b(\d{1,2})[-/](\d{1,2})[-/](\d{4})\b", t)
    if num:
        d1, d2, y = map(int, num.groups())
        # try DD/MM/YYYY
        try:
            dt = _dt.date(y, d2, d1)
            return dt.isoformat()
        except ValueError:
            # try MM/DD/YYYY
            try:
                dt = _dt.date(y, d1, d2)
                return dt.isoformat()
            except ValueError:
                pass

    # 5) As a last attempt, try strict ISO again (some strings may now be clean)
    try:
        dt = _dt.datetime.strptime(t, "%Y-%m-%d").date()
        return dt.isoformat()
    except ValueError:
        return None



import re

# Canonical names (Malaysia)
_CANONICAL_STATES = {
    "kualalumpur": "Kuala Lumpur",
    "wpkl": "Kuala Lumpur",
    "wilayahpersekutuankualalumpur": "Kuala Lumpur",

    "putrajaya": "Putrajaya",
    "labuan": "Labuan",

    "johor": "Johor",
    "kedah": "Kedah",
    "kelantan": "Kelantan",
    "melaka": "Melaka",       # a.k.a. Malacca
    "malacca": "Melaka",
    "negerisembilan": "Negeri Sembilan",
    "pahang": "Pahang",
    "pulaupinang": "Pulau Pinang",
    "penang": "Pulau Pinang",
    "perak": "Perak",
    "perlis": "Perlis",
    "selangor": "Selangor",
    "terengganu": "Terengganu",
    "sabah": "Sabah",
    "sarawak": "Sarawak",
}

def clean_state(value):
    """
    Normalize Malaysian 'state' names.
    - Converts dashes/underscores to spaces
    - Removes 'WP', 'W.P.', 'Wilayah Persekutuan' prefixes
    - Maps common aliases to canonical casing
    - Falls back to Title Case
    Returns None for blank/None.

    Examples:
      "Kuala-Lumpur" -> "Kuala Lumpur"
      "WP Kuala_Lumpur" -> "Kuala Lumpur"
      "penang" -> "Pulau Pinang"
      "Malacca" -> "Melaka"
    """
    if value is None:
        return None

    s = str(value).strip()
    if not s:
        return None

    # Remove common federal territory prefixes
    s = re.sub(r"\b(w\.?p\.?|wilayah\s+persekutuan)\b", "", s, flags=re.I)

    # Unify separators to spaces
    s = re.sub(r"[-_/.,]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return None

    # Canonical mapping by compact lowercase key
    key = re.sub(r"\s+", "", s).lower()
    if key in _CANONICAL_STATES:
        return _CANONICAL_STATES[key]

    # Fallback: Title Case (keeps multi-word like "Negeri Sembilan")
    return s.title()
