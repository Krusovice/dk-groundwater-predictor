import os

def detect_environment_name():
  try:
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
      environment_name='azure_databricks'
    else:
      environment_name = os.getenv('COMPUTERNAME')
    print(f'Environment detected: {environment_name}')
  except:
    raise Exception("Environment not found... is the computername hardcoded into environments.json?")
  return environment_name