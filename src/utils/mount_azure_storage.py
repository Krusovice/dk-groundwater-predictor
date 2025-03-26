from databricks.sdk.runtime import dbutils

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