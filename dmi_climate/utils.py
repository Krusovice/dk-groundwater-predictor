import os
import json
import sys

def detect_environment_name():
  try:
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
      environment_name='databricks'
    else:
      environment_name = os.getenv('COMPUTERNAME')
    print(f'Environment detected: {environment_name}')
  except:
    raise Exception("Environment not found... is the computername hardcoded into environments.json?")
  return environment_name

def data_filepaths(retry=False,client_id=None, tenant_id=None, client_secret=None):
  environment_name = detect_environment_name()

  print('Getting filepaths...')
  if environment_name == 'databricks':
    print('Returning databricks filepaths...')
    try:
      path_bronze = "dbfs:/mnt/bronze"
      path_silver = "dbfs:/mnt/silver"
      path_gold = "dbfs:/mnt/gold"
    except:
      if not retry:
        raise Exception("filepaths not found, trying to mount...")
        mount_azure_storage(client_id,tenant_id,client_secret)
        return retry == True
      else:
        raise Exception("Mounts failed after retry. Exiting.")
  else:
    try:
      print('Returning local local environment filepaths...')

      with open('environments.json', 'r') as f:
        environments = json.load(f)

      for key,value in environments.items():
        if key == environment_name:
          path_bronze = f'{value}//bronze'
          path_silver = f'{value}//silver'
          path_gold = f'{value}//gold'
    except:
      raise Exception("filepaths not found. Exiting.")

  return path_bronze, path_silver, path_gold

def mount_azure_storage(client_id,tenant_id,client_secret):
  try: 
    print('Mounting...')

    configs = {
      "fs.azure.account.auth.type": "OAuth",
      "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
      "fs.azure.account.oauth2.client.id": f"{client_id}",
      "fs.azure.account.oauth2.client.secret": f"{client_secret}",
      "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }

    for i in ['bronze', 'silver', 'gold']:
      dbutils.fs.mount(
          source = f"abfss://{i}@dmiclimatestorage.dfs.core.windows.net/",
          mount_point = f"/mnt/{i}",
          extra_configs = configs
      )
      print('Mounted!')
  except:
    raise Exception("Mounting error")





















  