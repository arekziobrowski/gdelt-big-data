#!/bin/bash

# Script installs necessary dependencies for initialize_etl.py

yum -y install epel-release
yum -y install python-pip
pip install avro
