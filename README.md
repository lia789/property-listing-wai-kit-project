
# **Property Listing Scraper**



## Setup to deploy on server

1. Clone the repository.
2. Create a `.env` file with your environment variables:
   ```bash
   SCRAPERAPI_KEY=<your_scraperapi_key>
   MYSQL_HOST=<your_mysql_host>
   MYSQL_PORT=<your_mysql_port>
   MYSQL_USER=<your_mysql_user>
   MYSQL_PASSWORD=<your_mysql_password>
   MYSQL_DB=<your_mysql_db>
   ```
3. Make sure .env file have all of the 3 directory



4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```



## Running the Scrapers

- To run the iProperty auction scraper:
   ```bash
   python iproperty_auction_listing/run_iproperty_auction.py
   ```
- To run the iProperty new listing scraper:
   ```bash
   python iproperty_new_listing/run_iproperty_new_listing.py
   ```
- To run the Property Guru scraper:
   ```bash
   python property_guru_new_listing/run_property_guru.py
   ```



## Deployment

To deploy the scraper on a server, make sure the server has Python and all required dependencies installed. Schedule the scrapers to run daily using cron jobs or any task scheduler.

## Contact
- **Email:** lia.dev750@gmail.com
- **Whatsapp:** +8801304246003
