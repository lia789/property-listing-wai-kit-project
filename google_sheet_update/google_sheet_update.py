# Import Libraries

import os
import pymysql
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
from dotenv import load_dotenv
from decimal import Decimal
from datetime import date


# Load Environment Variables
load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "property_listing")



# Google Sheets Configuration
SERVICE_ACCOUNT_FILE = 'property-listing-wai-kit.json'
SPREADSHEET_ID = '1tCKoVgl9Qiq9vccCmpSwyhvR35rdj6qxeWSzjE0-ZJg'
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]



# MySQL Connection
connection = pymysql.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DB,
    port=MYSQL_PORT,
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)


# Authenticate with Google Sheets API
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
service = build('sheets', 'v4', credentials=credentials)


# Fetch Data from MySQL Database
tables = {
    "iproperty-auction-listing": "iproperty-auction-listing",
    "iproperty-new-listing": "iproperty-new-listing",
    "property-guru-new-listing": "property-guru-new-listing"
}


new_data = {}
for table, sheet_name in tables.items():
    current_date = datetime.now().strftime('%Y-%m-%d')
    with connection.cursor() as cursor:
        query = f"""
            SELECT * FROM `{table}`
            WHERE data_scraping_date = %s
        """
        cursor.execute(query, (current_date,))
        new_data[sheet_name] = cursor.fetchall()

# Get Existing list_ids from Google Sheets
existing_list_ids = {}

for sheet_name in tables.values():
    # Get existing list_ids from Google Sheets (assumes list_id is in column A)
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{sheet_name}!A:A"
    ).execute()
    rows = result.get('values', [])
    # Store existing list_ids in a set, stripping spaces for consistency
    existing_list_ids[sheet_name] = {row[0].strip() for row in rows if row}
    # print(f"Existing list_ids for {sheet_name}: {existing_list_ids[sheet_name]}")

# Filter New Data to Avoid Duplicates
filtered_data = {}

for sheet_name, records in new_data.items():
    filtered_data[sheet_name] = [
        record for record in records if str(record['list_id']).strip() not in existing_list_ids[sheet_name]
    ]
    # print(f"Filtered data for {sheet_name}: {filtered_data[sheet_name]}")

# Update Google Sheets with New Data
for sheet_name, data in filtered_data.items():
    if data:
        # Convert Decimal values to float or str to ensure they are JSON serializable
        # Convert date objects to string (YYYY-MM-DD format)
        values = [
            [
                str(item[key]) if isinstance(item[key], date) else 
                float(item[key]) if isinstance(item[key], Decimal) else 
                item[key]
                for key in item
            ]
            for item in data
        ]
        
        body = {'values': values}
        
        # Append the new data to the corresponding sheet
        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A1",  # Adjust this to the correct starting cell if needed
            valueInputOption="RAW",
            body=body
        ).execute()
        # print(f"{len(data)} new records added to {sheet_name}.")
    else:
        print(f"No new data to add to {sheet_name}.")



# Close the MySQL Connection
connection.close()