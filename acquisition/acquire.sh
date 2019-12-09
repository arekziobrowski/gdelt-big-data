#!/bin/bash

./create_run_control_date.sh
python2.7 generate_tasks.py
python2.7 get_dictionaries.py
python2.7 download_csv.py

