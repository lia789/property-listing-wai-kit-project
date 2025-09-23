# spider.py

import os, json
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
import scrapy


from data_clean import (
    extract_area_state,
    clean_int_float,
    clean_bedrooms,
    clean_posted_date,
    clean_built_up_price,
    extract_lat_lng,
    analyze_description,
    clean_property_type,
    clean_property_title_type,
    extract_list_id,
)



API_KEY = str(os.getenv("SCRAPERAPI_KEY", ""))
PROXY_URL = f"http://scraperapi:{str(API_KEY)}@proxy-server.scraperapi.com:8001"



# Delete the log file if it exists before starting the spider (overwrite)
log_file_path = 'property_guru_new_listing_logs.txt'
if os.path.exists(log_file_path):
    os.remove(log_file_path)




# ---- Add/extend your sources here: each entry has a 'state' and a URL template with {page} ----
START_SOURCES = [
    {
        "state": "Kuala Lumpur",
        "url_template": "https://www.propertyguru.com.my/property-for-sale/{page}?_freetextDisplay=Kuala+Lumpur&isCommercial=false&order=desc&page=1&propertyTypeCode=APT&propertyTypeCode=CONDO&propertyTypeCode=FLAT&propertyTypeCode=SRES&propertyTypeGroup=N&regionCode=58jok&sort=date",
    },

    {
        "state": "Selangor",
        "url_template": "https://www.propertyguru.com.my/property-for-sale/{page}?_freetextDisplay=Selangor&isCommercial=false&order=desc&page=1&propertyTypeCode=APT&propertyTypeCode=CONDO&propertyTypeCode=FLAT&propertyTypeCode=SRES&propertyTypeGroup=N&regionCode=45nk1&sort=date",
    },
        {
        "state": "Johor",
        "url_template": "https://www.propertyguru.com.my/property-for-sale/{page}?_freetextDisplay=Johor&isCommercial=false&order=desc&page=1&propertyTypeCode=APT&propertyTypeCode=CONDO&propertyTypeCode=FLAT&propertyTypeCode=SRES&propertyTypeGroup=N&regionCode=2hh35&sort=date",
    },

        {
        "state": "Penang",
        "url_template": "https://www.propertyguru.com.my/property-for-sale/{page}?_freetextDisplay=Penang&isCommercial=false&order=desc&page=1&propertyTypeCode=APT&propertyTypeCode=CONDO&propertyTypeCode=FLAT&propertyTypeCode=SRES&propertyTypeGroup=N&regionCode=5qvq6&sort=date",
    },

    
]






class ExampleSpider(scrapy.Spider):
    name = "property_guru_batched"

    # Build requests for the first 10 pages for each source/state
    def start_requests(self):
    
        # Pagination instructions: wait 10s and ensure listing UL is present


        pag_headers = {
            # "x-sapi-render": "true",
            "x-sapi-device_type": "desktop",
            "x-sapi-retry_404": "true",
            "x-sapi-premium": "true",
        }

        for src in START_SOURCES:
            state = src["state"]
            tpl = src["url_template"]

            for page in range(1, 11):         # Integrate page number here
                url = tpl.format(page=page)
                yield scrapy.Request(
                    url,
                    headers=pag_headers,
                    meta={
                        "proxy": PROXY_URL,
                        "state": state,
                        "page": page,
                    },
                    callback=self.parse_pagination,
                    dont_filter=True,
                )








    # ---- Pagination page -> enqueue each listing ----
    def parse_pagination(self, response: scrapy.http.Response):
        listing_card_root = response.xpath("//div[@class='search-result-root']/div[@class='listing-card-banner-root']")

        for listing_card_link in listing_card_root:
            url = listing_card_link.xpath(".//a[@class='listing-card-link']/@href").get()
            

            # Sending listing page request
            if url:
                preview = {
                    "url": url,
                }


                # Detail page: wait for networkidle + static map image
                det_instr = [
                    {
                        "type": "wait_for_selector",
                        "selector": {"type": "css", "value": 'button[da-id="meta-table-see-more-btn"]'},
                        "timeout": 30
                    },
                    {
                        "type": "click",
                        "selector": {"type": "css", "value": 'button[da-id="meta-table-see-more-btn"]'}
                    },
                    {"type": "wait_for_event", "event": "stabilize", "seconds": 5}
                ]


                det_headers = {
                    "x-sapi-render": "true",
                    "x-sapi-premium": "true",
                    "x-sapi-instruction_set": json.dumps(det_instr),
                    "x-sapi-device_type": "desktop",
                    "x-sapi-retry_404": "true",
                    
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
        url = pv.get("url")


        # Data extraction xpath code
        list_id = extract_list_id(url)
        name = response.xpath('normalize-space(//h1/text())').get()
        address_row = response.xpath("//p[@da-id='property-address']/text()").get()
        area, state = extract_area_state(address_row)
        price = clean_int_float(response.xpath("normalize-space(//h2[@da-id='price-amount']/text()[2])").get())
        bed_rooms = clean_bedrooms(response.xpath("normalize-space(//div[@da-id='bedroom-amenity']/p/text())").get())
        built_up_size = clean_int_float(response.xpath("normalize-space(//div[@da-id='area-amenity']/p/text())").get())
        posted_date = clean_posted_date(response.xpath("//div[contains(text(), 'Listed on')]/text()").get())
        tenure = response.xpath("//img[@src='https://cdn.pgimgs.com/hive-ui-core/static/v1.6/icons/svgs/calendar-days-o.svg']/following-sibling::p[1]/text()").get()
        furnished_status = response.xpath("//img[@src='https://cdn.pgimgs.com/hive-ui-core/static/v1.6/icons/svgs/furnished-o.svg']/following-sibling::p[1]/text()").get()
        property_type = clean_property_type(response.xpath("//img[@src='https://cdn.pgimgs.com/hive-ui-core/static/v1.6/icons/svgs/home-open-o.svg']/following-sibling::p[1]/text()").get())
        property_title_type = clean_property_title_type(response.xpath("//img[@src='https://cdn.pgimgs.com/hive-ui-core/static/v1.6/icons/svgs/asterisk-o.svg']/../p[contains(text(), 'title')]/text()").get())
        bumi_lot = response.xpath("//img[@src='https://cdn.pgimgs.com/hive-ui-core/static/v1.6/icons/svgs/asterisk-o.svg']/../p[contains(text(), 'Bumi Lot')]/text()").get()
        built_up_price = clean_built_up_price(response.xpath("//div[@da-id='psf-amenity']//p/text()[2]").get())
        occupancy = response.xpath("//img[@src='https://cdn.pgimgs.com/hive-ui-core/static/v1.6/icons/svgs/people-behind-o.svg']/following-sibling::p[1]/text()").get()
        lat, lng = extract_lat_lng(response)

        # Description scraping
        try:
            description_raw = response.xpath("//h2[contains(text(), 'About this property')]/following-sibling::div[1]/text()").getall()
            if description_raw:
                description = ' '.join(description_raw)
            else:
                description = None
        except Exception as e:
            description = None


        des_result = analyze_description(description)
        new_project = des_result['new_project'] 
        auction = des_result['auction']
        below_market_value = des_result['below_market_value']
        urgent = des_result['urgent']


        agent_name = response.xpath("normalize-space(//div[@da-id='agent-name']/text())").get()
        agency_name = response.xpath("normalize-space(//div[@da-id='agent-agency-name']/text())").get()


        website_name = "propertyguru.com"
        data_scraping_date = datetime.now().strftime("%Y-%m-%d")




        item_dic = {
            "list_id":list_id,
            "name": name,
            "url": pv.get("url"),
            "area": area,
            "state": state,
            "price": price,
            "bed_rooms": bed_rooms,
            "built_up_size": built_up_size,
            "posted_date": posted_date,
            "tenure": tenure,
            "furnished_status": furnished_status,
            "property_type": property_type,
            # "land_title": land_title,
            "property_title_type": property_title_type,
            "bumi_lot": bumi_lot,
            "built_up_price": built_up_price,
            "occupancy": occupancy,
            # "unit_type": unit_type,
            "lat": lat,
            "lng": lng,
            "description": description,
            "new_project": new_project,
            "auction": auction,
            "below_market_value": below_market_value,
            "urgent": urgent,
            "agent_name": agent_name,
            "agency_name": agency_name,
            "website_name": website_name,
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




