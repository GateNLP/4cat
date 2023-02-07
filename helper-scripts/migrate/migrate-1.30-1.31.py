import configparser
import sys
import os

from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)) + "'/../..")
from common.lib.database import Database
from common.lib.logger import Logger

log = Logger(output=True)
import common.config_manager as config

db = Database(logger=log, dbname=config.get('DB_NAME'), user=config.get('DB_USER'), password=config.get('DB_PASSWORD'),
              host=config.get('DB_HOST'), port=config.get('DB_PORT'), appname="4cat-migrate")

# ---------------------------------------------
# Update config file with new events/drive settings
# ---------------------------------------------
config_path = Path(__file__).parent.parent.parent.joinpath("config/config.ini")

if config_path.exists():
    config_reader = configparser.ConfigParser()
    config_reader.read(config_path)

    if not config_reader.has_section("GOOGLE"):
        # Google OAuth setup
        config_reader.add_section('GOOGLE')
        config_reader['GOOGLE']['client_id'] = os.environ['FN_CLIENT_ID']
        config_reader['GOOGLE']['client_secret'] = os.environ['FN_CLIENT_SECRET']
        config_reader['GOOGLE']['auth_redirect_uri'] = os.environ['FN_AUTH_REDIRECT_URI']

    if not config.get("PATH_GDRIVE_ROOT"):
        config_reader['PATHS']['path_gdrive_root'] = 'fourcat-auto'

    # Save config file
    with open(config_path, 'w') as configfile:
        config_reader.write(configfile)
        print('Created config/config.ini file')
