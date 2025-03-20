from pathlib import Path
from dmi_climate.utils import data_filepaths

BASE_DIR = Path(__file__).resolve().parent.parent

CONFIG_DIR = BASE_DIR / 'config_files'
SECRETS_FILE = CONFIG_DIR / 'secrets.json'
ENVIRONMENTS_FILE = CONFIG_DIR / 'environments.json'
STATION_IDS_FILE = CONFIG_DIR / 'station_ids.json'

BRONZE_DIR, SILVER_DIR, GOLD_DIR = data_filepaths()

NOTEBOOKS_DIR = BASE_DIR / 'notebooks'