#!/bin/sh

# NOTE: this file must be run with sudo
if [ 0 -ne "$(id -u)" ]; then
    echo 'This file must be ran as sudo!'
    exit 1
fi

# environment
export AIRFLOW_HOME=${AIRFLOW_HOME:=~/airflow}
export AIRFLOW_CONFIG=$AIRFLOW_HOME/unittests.cfg

nose_args="--with-coverage \
--cover-erase \
--cover-html \
--cover-package=airflow \
--cover-html-dir=airflow/www/static/coverage \
-s \
-v \
--logging-level=DEBUG "

which airflow > /dev/null || python setup.py develop

echo "Initializing the DB"
yes | airflow resetdb
airflow initdb

echo "Starting the unit tests with the following nose arguments: "$nose_args
nosetests $nose_args tests/sudo.py
