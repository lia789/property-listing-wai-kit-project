from __future__ import annotations
import re
from typing import Optional, Tuple
from urllib.parse import urlparse
from datetime import datetime, timedelta


try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None



def split_area(area_state):
    try:
        parts = area_state.split(',')
        area = parts[0].strip()
        return area
    except:
        return None


def extract_list_id(url: str):
    try:
        path = urlparse(url).path  
        last_part = [p for p in path.split("/") if p][-1]  
        match = re.search(r"(\d+)$", last_part)  
        if match:
            return match.group(1)
        return None
    except Exception:
        return None

def clean_name(name_row):
    try:
        if name_row is None:
            return None
        s = str(name_row).replace("\xa0", " ").strip().strip(", ")
        if not s:
            return None
        first = s.split(",", 1)[0].strip()
        return first or None
    except Exception:
        return None
    

def clean_int_float(value_row):
    try:
        if value_row is None:
            return None
        s = str(value_row).strip()
        if not s:
            return None

        # Normalize thousand separators & weird spaces
        s = s.replace(",", "").replace("\xa0", " ")

        # Find first number (supports ".75", "2277", "2277.78")
        m = re.search(r"[-+]?\d*\.?\d+", s)
        if not m:
            return None

        n = float(m.group())
        return int(n) if n.is_integer() else n
    except Exception:
        return None


def clean_bedrooms(bed_rooms):
    try:
        if bed_rooms is None:
            return None
        if isinstance(bed_rooms, (int, float)) and not isinstance(bed_rooms, bool):
            return int(bed_rooms)

        s = str(bed_rooms).strip()
        if not s:
            return None

        if s.lower().startswith("room bed"):
            return 100
        s_no_space = s.replace(" ", "")

        if "+" in s_no_space:
            parts = s_no_space.split("+")
            nums = [int(p) for p in parts if p.isdigit()]
            return sum(nums) if nums else None
        matches = re.findall(r"\d+", s)
        if matches:
            return int(matches[0])

        return None
    except Exception:
        return None 
    

def extract_property_row(row: Optional[str]) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    try:
        if not row:
            return None, None, None

        # Normalize spaces
        s = str(row).replace("\xa0", " ").strip()

        # Split by bullets or 2+ spaces or pipes-with-spaces
        parts = [p.strip(" :\t\r\n") for p in re.split(r"\s*[•·]\s*| {2,}|\s\|\s", s) if p and p.strip()]
        if not parts:
            return None, None, None

        # 1) property_type = first non-empty part
        property_type = parts[0] if parts else None

        # Helpers
        def normalize_furnished(text: str) -> str:
            t = text.lower()
            if "unfurn" in t or "not furn" in t:
                return "Unfurnished"
            if "semi" in t or "partial" in t or "partly" in t:
                return "Partially Furnished"
            if "furni" in t:  # catch furnished / fully furnished
                return "Fully Furnished" if "full" in t else ("Furnished" if "furni" in t else text.strip())
            return text.strip()

        def parse_builtup_to_sqft(text: str) -> Optional[int]:
            t = text.lower()

            # extract first numeric token like 1,098 or 92 or 1200
            m = re.search(r"(\d[\d,\.]*)", t)
            if not m:
                return None
            raw = m.group(1)

            # basic numeric cleaning: drop commas
            num_txt = raw.replace(",", "")
            try:
                val = float(num_txt)
            except ValueError:
                return None

            # unit detection
            is_sqm = any(u in t for u in ["sqm", "m²", "sq m", "square meter", "square metres", "square meters"])
            # If explicitly sqft variants appear, keep as sqft
            is_sqft = any(u in t for u in ["sqft", "sq. ft", "sq ft", "square feet", "square foot"])

            if is_sqm and not is_sqft:
                val = val * 10.7639  # convert sqm -> sqft

            # round to closest int (DB expects INT)
            return int(round(val))

        built_up_size = None
        furnished_status = None

        for p in parts:
            pl = p.lower()

            if built_up_size is None and "built" in pl:  # catches Built-up / Built up
                candidate = parse_builtup_to_sqft(p)
                if candidate:
                    built_up_size = candidate

            if furnished_status is None and "furni" in pl:
                furnished_status = normalize_furnished(p)

            # Early exit if we have both
            if built_up_size is not None and furnished_status is not None:
                break

        return (property_type or None, built_up_size, furnished_status)
    except Exception:
        return None, None, None
    

def clean_posted_date(row: Optional[str],
                      tz: str = "Asia/Dhaka",
                      now: Optional[datetime] = None) -> Optional[str]:
    try:
        if not row:
            return None

        s = str(row).replace("\xa0", " ").strip()
        low = s.lower()

        
        if now is None:
            if ZoneInfo is not None:
                now = datetime.now(ZoneInfo(tz))
            else:
                now = datetime.now()

        if "today" in low:
            d = now.date()
            return d.strftime("%d %b %Y")
        if "yesterday" in low:
            d = (now - timedelta(days=1)).date()
            return d.strftime("%d %b %Y")
        
        m = re.search(r"\b(?P<d>\d{1,2})\s+(?P<m>[A-Za-z]{3,9}\.?)\s+(?P<y>\d{4})\b", s)
        if m:
            d = int(m.group("d"))
            y = int(m.group("y"))
            mword = m.group("m").rstrip(".")
            m3 = mword[:3].title()
            try:
                dt = datetime.strptime(f"{d:02d} {m3} {y}", "%d %b %Y")
                return dt.strftime("%d %b %Y")
            except ValueError:
                try:
                    dt = datetime.strptime(f"{d:02d} {mword.title()} {y}", "%d %B %Y")
                    return dt.strftime("%d %b %Y")
                except ValueError:
                    pass
        return None

    except Exception:
        return None





# --- Module export surface -----------------------------------------------------

__all__ = [
    "split_area",
    "extract_list_id",
    "clean_name",
    "clean_int_float",
    "clean_bedrooms",
    "extract_property_row",
    "clean_posted_date",
]
