import json
import requests
import os
import re
from datetime import date, datetime

# get max date from env var max_date
MAX_DATE = os.getenv("max_date")
max_date = date.fromisoformat(MAX_DATE)

# JSON file has URL for each dataset
with open("./sa_rents.json", "r") as file:
    data = json.load(file)

# Loop through each resource and download the file
for res in data["result"]["resources"]:
    url = res["url"]
    file_name = url.split("/")[-1]
    name = res["name"]

    # use regex to extract the date from private-rental-report-2024-09.xlsx
    date_quarter = re.search(r"(\d{4}-\d{2})([.])", file_name).group(1)
    year_  = date_quarter.split("-")[0]
    month_ = date_quarter.split("-")[1]
    current_date = date(year_, month_, 1)
    
    # check if date_ is greater then MAX_DATE
    if current_date < max_date:
        print(f"Skipping {name} as it is older than {MAX_DATE}")
        continue

    # Get the format of the file from the URL, format key is wrong in the JSON for some files!!
    file_format = url.split(".")[-1].lower()

    response = requests.get(url)
    file_name = f"sa_rents_data/{name.replace(' ', '_').lower()}.{file_format}"
    with open(file_name, "wb") as f:
        f.write(response.content)

    print(f"Downloaded {file_name}")
