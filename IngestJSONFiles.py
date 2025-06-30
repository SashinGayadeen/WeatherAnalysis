import requests
import json
from datetime import datetime
from pyspark.sql import Row 
from pyspark.sql.types import StringType
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from builtins import max, min

# ----------------------------
# Weather API Parameters
# ----------------------------
api_key = ""  # Replace with your actual key
locations = ["Durban", "Cape Town", "Johannesburg","Pretoria","East London","Polokwane","Nelspruit","Brits","Bloemfontein","196.21.8.1"]
start_date_input = "2025-01-01"


# ----------------------------
# Dynamic END DATE = yesterday
# ----------------------------

today = datetime.today()
end_date_input = today - timedelta(days=1)

start_date = datetime.strptime(start_date_input, "%Y-%m-%d")
end_date = end_date_input

print(f"Fetching from {start_date.date()} to {end_date.date()}")

# ----------------------------
# Loop month by month
# ----------------------------
for location in locations:
    current = start_date.replace(day=1)

    while current <= end_date:
        # Calculate this month's range
        month_start = current
        month_end = (current + relativedelta(months=1)) - timedelta(days=1)

        # Clip to true range
        range_start = max(month_start, start_date)
        range_end = min(month_end, end_date)

        range_start_str = range_start.strftime("%Y-%m-%d")
        range_end_str = range_end.strftime("%Y-%m-%d")

        # ----------------------------
        # Build WeatherAPI URL
        # ----------------------------
        url = (
            f"https://api.weatherapi.com/v1/history.json?"
            f"q={location}&dt={range_start_str}&end_dt={range_end_str}&key={api_key}"
        )

        print(f"Calling: {url}")

        # ----------------------------
        # Call the API
        # ----------------------------
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            json_str = json.dumps(data, indent=4)

            # ----------------------------
            # Save to Lakehouse Files
            # ----------------------------
            file_name = f"{location}_{range_start_str}_to_{range_end_str}.json"
            lakehouse_path = f"Files/raw_weather_json/{file_name}"

            mssparkutils.fs.put(lakehouse_path, json_str, overwrite=True)

            print(f"✅ JSON written to: {lakehouse_path}")

        else:
            print(
                f"❌ API call failed ({response.status_code}) for {range_start_str} to {range_end_str}"
            )

        # Next month
        current = current + relativedelta(months=1)
