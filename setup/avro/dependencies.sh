#!/bin/bash

# Script installs necessary dependencies for initialize_etl.py

yum install epel-release
yum install python-pip
pip install avro
