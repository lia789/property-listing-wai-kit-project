import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
import scrapy
import time


from data_clean import (
    split_area,
    extract_list_id,
    clean_name,
    clean_int_float,
    clean_bedrooms,
    extract_property_row,
    clean_posted_date,
)


# Settings
API_KEY = str(os.getenv("SCRAPERAPI_KEY", ""))
PROXY_URL = f"http://scraperapi:{str(API_KEY)}@proxy-server.scraperapi.com:8001"
# MAX_PAGES = 5


# Delete the log file if it exists before starting the spider (overwrite)
log_file_path = 'iproperty_auction_logs.txt'
if os.path.exists(log_file_path):
    os.remove(log_file_path)



# Request URLs
START_SOURCES = [
    {
        "state": "Kuala-Lumpur",
        "url_template": "https://www.iproperty.com.my/sale/kuala-lumpur-58jok/apartment-flat/?subChannel=auction&l1&page={page}",
    },

    {
        "state": "Selangor",
        "url_template": "https://www.iproperty.com.my/sale/selangor-45nk1/apartment-flat/?subChannel=auction&l1&page={page}",
    },

    {
        "state": "Johor",
        "url_template": "https://www.iproperty.com.my/sale/johor-2hh35/apartment-flat/?subChannel=auction&l1&page={page}",
    },


    {
        "state": "Penang",
        "url_template": "https://www.iproperty.com.my/sale/penang-5qvq6/apartment-flat/?subChannel=auction&l1&page={page}",
    },



]




class ExampleSpider(scrapy.Spider):
    name = "iproperty_batched"

    def start_requests(self):
        pag_headers = {
            # "x-sapi-render": "true",
            "x-sapi-device_type": "desktop",
            "x-sapi-retry_404": "true",
            # "x-sapi-premium": "true",
            "x-sapi-ultra_premium": "true",
        }

        for src in START_SOURCES:
            state = src["state"]
            tpl = src["url_template"]



            for page in range(1, 5):
                url = tpl.format(page=page)
                time.sleep(0.1)

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



    def parse_pagination(self, response: scrapy.http.Response):
        state = response.meta.get("state")


        # Data extraction xpath
        ul = response.xpath("//ul[@data-test-id='listing-list']/li")
        for li in ul:
            url = li.xpath(".//a[@class='depth-listing-card-link']/@href").get()
            list_id = extract_list_id(url)

            name_row = li.xpath(".//h2/text()").get()
            name = clean_name(name_row)

            area = split_area(li.xpath(".//div[contains(@class, 'AddressWrapper')]/text()").get())
            price = clean_int_float(li.xpath(".//li[contains(@class, 'ListingPricestyle__ItemWrapper-etxdML')]/text()").get())
            bed_rooms = clean_bedrooms(li.xpath(".//li[contains(@class, 'bedroom-facility')]/text()").get())
            birth_rooms = li.xpath(".//li[contains(@class, 'bathroom-facility')]/text()").get()
            perking = li.xpath(".//li[contains(@class, 'carPark-facility')]/text()").get()
            posted_date_raw = li.xpath(".//p[contains(text(), 'Posted')]/text()").get()
            extract_property_row_string = li.xpath(".//p[contains(@class, 'ListingAttributesstyle__ListingAttrsDescriptionItemWrapper-cCDpp') and contains(text(), 'Built-up')]/text()").get()
            property_type, built_up_size, furnished_status = extract_property_row(extract_property_row_string)
            data_scraping_date = datetime.now().strftime("%Y-%m-%d")
            posted_date = clean_posted_date(posted_date_raw)


            item_dic = {
                "list_id": list_id,
                "name": name,
                "url": url,
                "area": area,
                "state": state,
                "price": price,
                "bed_rooms": bed_rooms,
                "built_up_size": built_up_size,
                "posted_date": posted_date,
                "birth_room": birth_rooms,
                "perking": perking,
                "property_type": property_type,
                "furnished_status": furnished_status,
                "data_scraping_date": data_scraping_date

            }

            yield item_dic


    


    custom_settings = {
        "CONCURRENT_REQUESTS": 10,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 10,
        "DOWNLOAD_TIMEOUT": 300,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 10,
        "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 525, 526, 527, 599, 418],
        "ITEM_PIPELINES": {"db_pipeline.MySQLStorePipelineBatched": 300,},

        # Logs
        "LOG_ENABLED": True,
        "LOG_LEVEL": "DEBUG",
        "LOG_FILE": log_file_path




    }





