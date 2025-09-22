# importing the requests library 
import re
from urllib.parse import urlparse
from parsel import Selector






# Data clean functions
def extract_area_state(address_row):
    try:
        # Split the address by comma
        address_parts = address_row.split(',')
        
        # Check if there are at least 2 parts (area and state)
        if len(address_parts) >= 2:
            # Trim leading/trailing spaces
            area = address_parts[-2].strip()
            state = address_parts[-1].strip()
            return area, state
        else:
            return None, None
    except:
        return None, None



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



def clean_posted_date(posted_date):
    try:
        # Use regex to extract the date part (matches the format "15 Sep 2025")
        match = re.search(r'\d{1,2} \w{3} \d{4}', posted_date)
        return match.group(0) if match else None
    except Exception as e:
        return None

def clean_built_up_price(built_up_price):
    try:
        # Use regex to remove all non-numeric characters, but keep the decimal point
        cleaned_value = re.sub(r'[^\d.]', '', built_up_price)
        return float(cleaned_value) if cleaned_value.replace('.', '', 1).isdigit() else None
    except:
        return None


def extract_lat_lng(response: Selector):
    try:
        scripts = response.xpath(
            "//script[contains(text(),'center') or contains(text(),'gmapSdkAPIKey')]/text()"
        ).getall()
        blob = "\n".join(scripts)

        # 1) Most common: ..."center":{"lat":3.131314,"lng":101.684121}
        m = re.search(
            r'"center"\s*:\s*\{\s*"lat"\s*:\s*([-+]?\d+(?:\.\d+)?)\s*,\s*"lng"\s*:\s*([-+]?\d+(?:\.\d+)?)',
            blob, flags=re.DOTALL
        )
        # 2) Fallback: any "lat":X,"lng":Y pair (keeps first occurrence)
        if not m:
            m = re.search(
                r'"lat"\s*:\s*([-+]?\d+(?:\.\d+)?)\s*,\s*"lng"\s*:\s*([-+]?\d+(?:\.\d+)?)',
                blob, flags=re.DOTALL
            )

        if m:
            lat, lng = float(m.group(1)), float(m.group(2))
            return lat, lng

        a_text = " ".join(response.xpath("//a/@href").getall())
        m = re.search(
            r'https?://(?:www\.)?google\.[^/\s]+/maps\?[^"\s]*\bq=([-+]?\d+(?:\.\d+)?),\s*([-+]?\d+(?:\.\d+)?)',
            a_text
        )
        if m:
            return float(m.group(1)), float(m.group(2))

        return None, None
    except Exception:
        return None, None



def analyze_description(description):
    # Define the keyword sets for each category
    new_launch_keywords = ["new launch", "rebate", "direct developer", "early bird", "sale package", 
                           "new project", "free spa legal", "free legal", "free loan legal"]
    auction_keywords = ["auction", "lelong", "reserve price", "bid", "bidding", "bidder", "bids"]
    urgent_sales_keywords = ["urgent", "must sell", "quick sale"]
    below_market_keywords = ["below market", "discount", "bargain", "fire sale"]

    # Normalize description
    try:
        text = description.lower()
    except Exception:
        text = ""

    # Helper to return 1 if any keyword matches, else 0
    def flag(keywords):
        return 1 if any(k in text for k in keywords) else 0

    return {
        "new_project": flag(new_launch_keywords),
        "auction": flag(auction_keywords),
        "below_market_value": flag(below_market_keywords),
        "urgent": flag(urgent_sales_keywords),
    }


def clean_property_type(property_type):
    try:
        cleaned_property_type = property_type.replace("for sale", "").strip()
        return cleaned_property_type
    except:
        return None


def clean_property_title_type(property_title_type):
    try:
        cleaned_property_title_type = property_title_type.replace("title", "").strip()
        return cleaned_property_title_type
    except Exception as e:
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
    




# --- Module export surface -----------------------------------------------------

__all__ = [
    "extract_area_state",
    "clean_int_float",
    "clean_bedrooms",
    "clean_posted_date",
    "clean_built_up_price",
    "extract_lat_lng",
    "analyze_description",
    "clean_property_type",
    "clean_property_title_type",
    "extract_list_id",
]


