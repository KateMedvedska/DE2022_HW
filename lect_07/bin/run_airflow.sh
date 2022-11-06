#!/usr/bin/env bash
# NB:install Apache Airflow first using install_airflow.sh script

# TODO: Change this to the path where airflow directory is located
# (default is ~/airflow)
export AIRFLOW_HOME=/Users/k.medvedska/Repositories/DE2022_HW/lect_07/airflow
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export no_proxy="*"

airflow standalone
