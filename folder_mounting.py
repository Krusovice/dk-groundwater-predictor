configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "<APP-ID>",
  "fs.azure.account.oauth2.client.secret": "<CLIENT-SECRET>",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<TENANT-ID>/oauth2/token"
}


dbutils.fs.mount(
	  source = "abfss://weather-data@dmiweatherstorage.dfs.core.windows.net/",
	  mount_point = "/mnt/weather-data",
	  extra_configs = configs
)