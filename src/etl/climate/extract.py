import requests

def extract_dmi_metObs(secret_key,url,**kwargs):
    headers = {
        "X-Gravitee-Api-Key": secret_key
    }
    
    response = requests.get(url, params=kwargs, headers=headers)
    
    return response.json()


