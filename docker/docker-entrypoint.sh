#!/bin/sh
set -e

exit_backend() {
  echo "Exiting backend"
  python3 4cat-daemon.py stop
  exit 0
}

trap exit_backend INT TERM

# Handle any options
while test $# != 0
do
    case "$1" in
    -p ) # set public option to use public IP address as SERVER_NAME
        echo 'Setting SERVER_NAME to public IP'
        SERVER_NAME=$(curl -s https://api.ipify.org);;
    * )  # Invalid option
        echo "Error: Invalid option"
        exit;;
    esac
    shift
done

## todo: change to envvars
echo "Waiting for postgres..."
while ! nc -z $DB_HOST $DB_PORT; do
  sleep 0.1
done
echo "PostgreSQL started"

# Create Database if it does not already exist
if [ `psql --host=$DB_HOST --port=$DB_PORT --user=$POSTGRES_USER --dbname=$POSTGRES_DB -tAc "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'jobs')"` = 't' ]; then
  # Table already exists
  echo "Database already created"
else
  echo "Creating Database"
  # Seed DB
  cd /usr/src/app && psql --host=$DB_HOST --port=$DB_PORT --user=$POSTGRES_USER --dbname=$POSTGRES_DB < backend/database.sql
  # No database exists, new build, no need to migrate so create .current-version file
  cp VERSION config/.current-version
fi

# If backend did gracefully shutdown, PID lockfile remains; Remove lockfile
rm -f ./backend/4cat.pid

# Run migrate prior to setup (old builds pre 1.26 may not have config_manager)
python3 helper-scripts/migrate.py -y

# Run docker_setup to update any environment variables if they were changed
python3 docker/docker_setup.py

echo 'Starting app'
echo "4CAT is accessible at:"
echo "http://$SERVER_NAME:$PUBLIC_PORT"
echo ''

# Start 4CAT backend
python3 4cat-daemon.py start

# Hang out until SIGTERM received
while true; do
    sleep 1
done
