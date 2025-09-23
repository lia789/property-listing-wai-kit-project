# spider.py

import os, json
from datetime import datetime
from urllib.parse import urljoin
from dotenv import load_dotenv
load_dotenv()
import scrapy


from data_clean import (
    split_area, extract_list_id, get_condo_name, extract_lat_long_from_url,
    analyze_description, clean_bedrooms, clean_int_float,
)


API_KEY = str(os.getenv("SCRAPERAPI_KEY", ""))
WEBSITE_NAME = "iproperty.com"
PROXY_URL = f"http://scraperapi:{str(API_KEY)}@proxy-server.scraperapi.com:8001"
# MAX_PAGES = 10


# Delete the log file if it exists before starting the spider (overwrite)
log_file_path = 'iproperty_new_listing_logs.txt'
if os.path.exists(log_file_path):
    os.remove(log_file_path)




# ---- Add/extend your sources here: each entry has a 'state' and a URL template with {page} ----
START_SOURCES = [
    {
        "state": "Kuala Lumpur",
        "url_template": "https://www.iproperty.com.my/sale/kuala-lumpur-58jok/apartment-flat/?l1&page={page}",
    },
    {
        "state": "Selangor",
        "url_template": "https://www.iproperty.com.my/sale/selangor-45nk1/apartment-flat/?l1&page={page}",
    },

    {
        "state": "Johor",
        "url_template": "https://www.iproperty.com.my/sale/johor-2hh35/apartment-flat/?l1&page={page}",
    },
    {
        "state": "Penang",
        "url_template": "https://www.iproperty.com.my/sale/penang-5qvq6/apartment-flat/?l1&page={page}",
    },    
]






class ExampleSpider(scrapy.Spider):
    name = "iproperty_batched"

    
    def start_requests(self):
        pag_headers = {
            "x-sapi-render": "true",
            "x-sapi-device_type": "desktop",
            "x-sapi-retry_404": "true",
            "x-sapi-premium": "true",
        }

        for src in START_SOURCES:
            state = src["state"]
            tpl = src["url_template"]


            for page in range(1, 11):
                url = tpl.format(page=page)
                yield scrapy.Request(
                    url,
                    headers=pag_headers,
                    meta={
                        "proxy": PROXY_URL,
                        "state": state,
                        "page": page,
                        "url_template": tpl,
                    },
                    callback=self.parse_pagination,
                    dont_filter=True,
                )






    # ---- Pagination page -> enqueue each listing ----
    def parse_pagination(self, response: scrapy.http.Response):
        state = response.meta.get("state")

        li_nodes = response.xpath("//ul[@data-test-id='listing-list']/li")

        for li in li_nodes:
            href = li.xpath(".//a[@class='depth-listing-card-link']/@href").get()
            url = urljoin(response.url, href) if href else None

            area = split_area(li.xpath(".//div[contains(@class, 'AddressWrapper')]/text()").get())
            price = clean_int_float(li.xpath(".//li[contains(@class, 'ListingPricestyle__ItemWrapper-etxdML')]/text()").get())
            bed_rooms = clean_bedrooms(li.xpath(".//li[@class='ListingAttributesstyle__ListingAttrsFacilitiesItemWrapper-klELeo bvrUdi attributes-facilities-item-wrapper bedroom-facility']/text()").get())
            list_id = extract_list_id(url) if url else None
            
            # Sending listing page request
            if url:
                preview = {
                    "list_id": list_id,
                    "url": url,
                    "area": area,
                    "state": state,
                    "price": price,
                    "bed_rooms": bed_rooms,
                }


                # Detail pa"x-sapi-premium": "true",ge: wait for networkidle + static map image
                det_instr = [
                    {"type": "wait_for_event", "event": "networkidle", "timeout": 10},
                    {
                        "type": "wait_for_selector",
                        "selector": {"type": "xpath", "value": "//img[contains(@src, 'https://maps.googleapis.com/maps/api/staticmap')]"},
                        "timeout": 20,
                    },
                ]
                det_headers = {
                    "x-sapi-render": "true",
                    "x-sapi-instruction_set": json.dumps(det_instr),
                    "x-sapi-device_type": "desktop",
                    "x-sapi-retry_404": "true",
                    "x-sapi-premium": "true",
                }

                yield scrapy.Request(
                    url,
                    headers=det_headers,
                    meta={
                        "proxy": PROXY_URL,
                        "preview": preview,
                    },
                    callback=self.parse_detail,
                )




    # ---- Listing page -> build full item ----
    def parse_detail(self, response: scrapy.http.Response):

        # Reding meta columns
        m = response.meta
        pv = m.get("preview", {})


        # Data extraction xpath code
        name_row = response.xpath("normalize-space(//h1/text())").get()
        name = get_condo_name(name_row)

        tenure = response.xpath("normalize-space(//div[contains(text(), 'Tenure')]/following-sibling::div[1]/text())").get()
        furnished_status = response.xpath("normalize-space(//div[contains(text(), 'Furnishing')]/following-sibling::div[1]/text())").get()
        property_type = response.xpath("normalize-space(//div[contains(text(), 'Property type')]/following-sibling::div[1]/text())").get()
        land_title = response.xpath("normalize-space(//div[contains(text(), 'Land title')]/following-sibling::div[1]/text())").get()
        property_title_type = response.xpath("normalize-space(//div[contains(text(), 'Property title type')]/following-sibling::div[1]/text())").get()
        bumi_lot = response.xpath("normalize-space(//div[contains(text(), 'Bumi lot')]/following-sibling::div[1]/text())").get()

        built_up_size = response.xpath("normalize-space(//div[contains(text(), 'Built-up size')]/following-sibling::div[1]/text())").get()

        built_up_price = response.xpath("normalize-space(//div[contains(text(), 'Built-up price')]/following-sibling::div[1]/text())").get()
        occupancy = response.xpath("normalize-space(//div[contains(text(), 'Occupancy')]/following-sibling::div[1]/text())").get()
        unit_type = response.xpath("normalize-space(//div[contains(text(), 'Unit type')]/following-sibling::div[1]/text())").get()
        posted_date = response.xpath("normalize-space(//div[contains(text(), 'Posted date')]/following-sibling::div[1]/text())").get()



        # Map → lat/lng
        google_maps_link = response.xpath("//img[contains(@src, 'https://maps.googleapis.com/maps/api/staticmap')]/@src").get()
        lat, lng = extract_lat_long_from_url(google_maps_link)



        # Description scraping
        try:
            description_raw = response.xpath("//p[@class='sc-c20be062-3 hqRhiu']/text()").getall()
            if description_raw:
                description = ' '.join(description_raw)
            else:
                description = None
        except Exception as e:
            description = None


        # Description analysis
        des_result = analyze_description(description)
        new_project = des_result['new_project'] 
        auction = des_result['auction']
        below_market_value = des_result['below_market_value']
        urgent = des_result['urgent'] 


        # Agent details
        agent_name = response.xpath("normalize-space(//div[contains(text(), 'REN')]/../a/text())").get()
        if not agent_name:
            agent_name = response.xpath("normalize-space(//a[contains(@class, 'sc-3020a48f-3')]/text())").get()

        agency_name = response.xpath("normalize-space(//div[@class='sc-506b84eb-1 cfWLHM']/text())").get()
        if not agency_name:
            agency_name = response.xpath("normalize-space(//div[contains(text(), 'Private Advertiser')]/text())").get()


        data_scraping_date = datetime.now().strftime("%Y-%m-%d")
        price = pv.get("price")
        bed_rooms = pv.get("bed_rooms")






        item_dic = {
            "list_id": pv.get("list_id"),
            "name": name,
            "url": pv.get("url"),
            "area": pv.get("area"),
            "state": pv.get("state"),
            "price": clean_int_float(price),
            "bed_rooms": bed_rooms,
            "built_up_size": clean_int_float(built_up_size),
            "posted_date": posted_date,
            "tenure": tenure,
            "furnished_status": furnished_status,
            "property_type": property_type,
            "land_title": land_title,
            "property_title_type": property_title_type,
            "bumi_lot": bumi_lot,
            "built_up_price": clean_int_float(built_up_price),
            "occupancy": occupancy,
            "unit_type": unit_type,
            "lat": lat,
            "lng": lng,
            "description": description,
            "new_project": new_project,
            "auction": auction,
            "below_market_value": below_market_value,
            "urgent": urgent,
            "agent_name": agent_name,
            "agency_name": agency_name,
            "website_name": "iproperty.com",
            "data_scraping_date": data_scraping_date,

        }

        yield item_dic








    # ---- Scrapy settings kept simple; DB pipeline is in db_pipeline.py ----
    custom_settings = {
        # Throughput
        "CONCURRENT_REQUESTS": 50,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 50,
        "DOWNLOAD_TIMEOUT": 100,

        # Retry 10x on 500-class and 429 (matches your "retry 10 times" ask)
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 10,
        "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 525, 526, 527, 599, 418,],

        # DB pipeline
        "ITEM_PIPELINES": {"db_pipeline.MySQLStorePipelineBatched": 300,},

        # Logs
        "LOG_ENABLED": True,
        "LOG_LEVEL": "INFO",
        "LOG_FILE": log_file_path, 
    }



