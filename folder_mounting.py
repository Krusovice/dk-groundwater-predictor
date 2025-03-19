
client_id = dbutils.secrets.get(scope = "dmi-climate-scope", key = "dmi-climate-client-id")
tenant_id = dbutils.secrets.get(scope = "dmi-climate-scope", key = "dmi-climate-tenant-id")
client_secret = dbutils.secrets.get(scope = "dmi-climate-scope", key = "dmi-climate-secret")

def detect_environment():
  if "DATABRICKS_RUNTIME_VERSION" in os.environ:
    environment='databricks'
  else:
    environment = os.getenv('COMPUTERNAME')
  return environment  

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

    for i in ['bronze', 'silver', 'gold']
    dbutils.fs.mount(
        source = f"abfss://{i}@dmiclimatestorage.dfs.core.windows.net/",
        mount_point = f"/mnt/{i}",
        extra_configs = configs
    )
    print('Mounted!')
  except:
    raise Exception("Mounting error")

def locate_mounts(envrionment):
  if envrionment == 'databricks':
    try:
      path_bronze = "dbfs:/mnt/bronze"
      path_silver = "dbfs:/mnt/silver"
      path_gold = "dbfs:/mnt/gold"
    except:
      if not retry:
        raise Exception("Mounts not found, trying to mount...")
        mount_azure_storage(client_id,tenant_id,client_secret)
        return retry = True
      else:
        raise Exception("Mounts failed after retry. Exiting...")






















  