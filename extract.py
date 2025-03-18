import json

# Indlæs secrets.json
with open('secrets.json') as f:
    secrets = json.load(f)

import requests

url = "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"

parameterIds = ["precip_past1h","temp_mean_past1h","temp_soil_mean_past1h"]

params = {
    "bbox": "12.3,55.6,12.7,55.8",
    "period": "latest-day",
    "parameterId": "precip_past1h",
    "limit": 20  # Antal poster du vil hente
}

headers = {
    "X-Gravitee-Api-Key": secrets["metObs-api-key"]
}


response = requests.get(url, params=params, headers=headers)

data = response.json()
print(data)