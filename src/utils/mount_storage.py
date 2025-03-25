import json
from src.utils.environments import detect_environment_name
from config.paths import ENVIRONMENTS_FILE

def mount_azure_storage(folder_name, client_id,tenant_id,client_secret):
  """
  Mounting a folder onto Azure Storage. The folder is mounted in root directory.
  """

  try:
    print(f'Mounting {folder_name}...')

    configs = {
      "fs.azure.account.auth.type": "OAuth",
      "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
      "fs.azure.account.oauth2.client.id": f"{client_id}",
      "fs.azure.account.oauth2.client.secret": f"{client_secret}",
      "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }

    dbutils.fs.mount(
        source = f"abfss://{folder_name}@dmiclimatestorage.dfs.core.windows.net/",
        mount_point = f"/mnt/{folder_name}",
        extra_configs = configs
    )
    print(f'Mounted {folder_name}!')
  except:
    raise Exception("Mounting error")

def azure_databricks_filepaths(retry=False,client_id=None, tenant_id=None, client_secret=None):
  """
  Returning bronze, silver and gold directories in a dictionary. These are all mounted to Azure Storage. 
  """

  print('Returning databricks filepaths...')
  paths = {}
  for i in ['bronze','silver','gold']:
    try:
        dbutils.fs.ls(f"/mnt/{i}")
        paths[i] = f"/mnt/{i}"
    except:
      if not retry:
        raise Exception("filepaths not found, trying to mount...")
        mount_azure_storage(i,client_id,tenant_id,client_secret)
        return retry == True
      else:
        raise Exception("Mounts failed after retry. Exiting.")  


def local_filepaths(environment_name):
  """
  Returning bronze, silver and gold directories in a dictionary.
  The directory locations are hardcoded in config.environments.json.
  """
  try:
    with open(ENVIRONMENTS_FILE, 'r') as f:
      environments = json.load(f)

    paths = {}
    for key,value in environments.items():
      if key == environment_name:
        paths['bronze'] = f'{value}//bronze'
        paths['silver'] = f'{value}//silver'
        paths['gold'] = f'{value}//gold'
  except:
    raise Exception("filepaths not found. Exiting.")

  return paths
 