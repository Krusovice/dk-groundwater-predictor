from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

CONFIG_DIR = BASE_DIR / 'config'
SECRETS_FILE = CONFIG_DIR / 'secrets.json'
ENVIRONMENTS_FILE = CONFIG_DIR / 'environments.json'
STATION_IDS_FILE = CONFIG_DIR / 'station_ids.json'

NOTEBOOKS_DIR = BASE_DIR / 'notebooks'