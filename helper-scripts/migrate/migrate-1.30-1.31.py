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
#   Telegram events and google drive updates
# ---------------------------------------------
print(" Checking if datasets table has a column 'is_continuous'...")
has_column = db.fetchone("SELECT COUNT(*) AS num FROM information_schema.columns WHERE table_name = 'datasets' AND column_name = 'is_continuous'")
if has_column["num"] == 0:
    print("  ...No, adding.")
    db.execute("ALTER TABLE datasets ADD COLUMN is_continuous BOOLEAN DEFAULT FALSE")
    db.commit()
else:
    print("  ...Yes, nothing to update.")

print(" Checking if datasets table has a column 'num_files'...")
has_column = db.fetchone("SELECT COUNT(*) AS num FROM information_schema.columns WHERE table_name = 'datasets' AND column_name = 'num_files'")
if has_column["num"] == 0:
    print("  ...No, adding.")
    db.execute("ALTER TABLE datasets ADD COLUMN num_files INTEGER DEFAULT 1")
    db.commit()
else:
    print("  ...Yes, nothing to update.")

print(" Checking if datasets table has a column 'drive_dir_id'...")
has_column = db.fetchone("SELECT COUNT(*) AS num FROM information_schema.columns WHERE table_name = 'datasets' AND column_name = 'drive_dir_id'")
if has_column["num"] == 0:
    print("  ...No, adding.")
    db.execute("ALTER TABLE datasets ADD COLUMN drive_dir_id TEXT")
    db.commit()
else:
    print("  ...Yes, nothing to update.")

# ---------------------------------------------
# Adding a subfile table
# ---------------------------------------------

print("  Checking if subfiles table exist... ", end="")
annotations_table = db.fetchone("SELECT EXISTS ( SELECT FROM information_schema.tables WHERE table_name = 'subfiles')")

if not annotations_table["exists"]:
    print("No, creating it now.")

    db.execute("""CREATE TABLE IF NOT EXISTS subfiles (
        id               SERIAL PRIMARY KEY,
        key              text,
        file_path        text,
        file_type	     text DEFAULT '',
        saved_date	     integer,
        uploaded_date    integer,
        owner            VARCHAR DEFAULT 'anonymous')""")

else:
    print("Indeed it exists. Moving on.")

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
