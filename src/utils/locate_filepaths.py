import json
from src.utils.mount_azure_storage import mount_azure_storage
from config.paths import ENVIRONMENTS_FILE
from databricks.sdk.runtime import dbutils
from src.utils.detect_environments import detect_environment_name



def storage_filepaths(client_id=None, tenant_id=None, client_secret=None):
  """
  Returning bronze, silver and gold directories in a dictionary. These are all mounted to Azure Storage.

  client_id, tenant_id and client secrets are optional defaulting to None, so the function can be used both to mount in one notebook and to provide filepaths in later notebooks.
  """
  environment = detect_environment_name()

  if environment == "azure_databricks":
    filepaths = azure_databricks_filepaths(client_id, tenant_id, client_secret)
  else:
    filepaths = local_filepaths()
  return filepaths



def is_mounted(mount_point: str) -> bool:
    return any(m.mountPoint == mount_point for m in dbutils.fs.mounts())



def azure_databricks_filepaths(client_id=None, tenant_id=None, client_secret=None):
  print('Returning databricks filepaths...')
  filepaths = {}
  for i in ['bronze','silver','gold']:
    
    # Mounting, if the filepath is not found already
    if not is_mounted(f"/mnt/{i}"):
      print("filepaths not found, trying to mount...")
      try:
        mount_azure_storage(i,client_id,tenant_id,client_secret)
      except:
        raise Exception("Mounting failed. Exiting.")
    
    filepaths[i] = f"/mnt/{i}"
    print(f'Found filepath {i}!')
  return filepaths



def local_filepaths(environment_name):
  """
  Returning bronze, silver and gold directories in a dictionary.
  The directory locations are hardcoded in config.environments.json.
  """
  try:
    with open(ENVIRONMENTS_FILE, 'r') as f:
      environments = json.load(f)

    filepaths = {}
    for key,value in environments.items():
      if key == environment_name:
        filepaths['bronze'] = f'{value}//bronze'
        filepaths['silver'] = f'{value}//silver'
        filepaths['gold'] = f'{value}//gold'
  except:
    raise Exception("filepaths not found. Exiting.")
  return filepaths
 