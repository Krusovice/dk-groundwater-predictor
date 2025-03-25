import requests

def extract_dmi_metObs(parameterId,bbox,period,limit,url,secret_key):
    params = {
        "bbox": bbox,
        "period": period,
        "parameterId": parameterId,
        "limit": limit
    }
    
    headers = {
        "X-Gravitee-Api-Key": secret_key
    }
    
    response = requests.get(url, params=params, headers=headers)
    
    return response.json()


