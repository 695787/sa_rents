import json
import requests

# JSON file has URL for each dataset
with open("./etl/sa_datasets.json", "r") as file:
    data = json.load(file)

# Loop through each resource and download the file
for res in data["result"]["resources"]:
    url = res["url"]
    file_name = url.split("/")[-1]
    name = res["name"]

    # Get the format of the file from the URL, format key is wrong in the JSON for some files!!
    file_format = url.split(".")[-1].lower()

    response = requests.get(url)
    file_name = f"sa_rents_data/{name.replace(' ', '_').lower()}.{file_format}"
    with open(file_name, "wb") as f:
        f.write(response.content)

    print(f"Downloaded {file_name}")
