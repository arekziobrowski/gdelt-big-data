#!/bin/bash
sudo yum -y install gcc openssl-devel bzip2-devel wget libffi-devel
cd /tmp
wget https://www.python.org/ftp/python/3.7.3/Python-3.7.3.tgz
tar xzf Python-3.7.3.tgz
cd Python-3.7.3
./configure --enable-optimizations
sudo make altinstall
rm /tmp/Python-3.7.3.tgz
