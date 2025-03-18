import json
from extract_functions import extract_dmi_metObs
import pandas as pd

with open('secrets.json') as f:
    secrets = json.load(f)


url = "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
parameterIds = ["sun_last1h_glob"]#,"temp_mean_past1h","temp_soil_mean_past1h",'sun_last1h_glob','pressure']
bbox="12.3,55.6,12.7,56.0"
#period="latest-day"
period='latest-day'
limit=100
secret_key=secrets["metObs-api-key"]

for i in parameterIds:    
    data = extract_dmi_metObs(i,bbox,period,limit,url,secret_key)

features = data.get('features')
records = [item['properties'] for item in features]
df = pd.DataFrame(records)

stationIds = {
    "06188": "Sjælsmark",
    "06180": "Cph Airport",
    "05735": "Botanical Garden",
    "06186": "Landbohøjskolen",
    "06181": "Jægersborg",
    "06187": "Københavns Toldbod",
    }

