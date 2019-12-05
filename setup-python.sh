#!/bin/bash
sudo yum -y install gcc openssl-devel bzip2-devel wget libffi-devel
cd /tmp
wget https://www.python.org/ftp/python/2.7.17/Python-2.7.17.tgz
tar xzf Python-2.7.17.tgz
cd Python-2.7.17
./configure --enable-optimizations
sudo make altinstall
rm /tmp/Python-2.7.17.tgz
python2.7 -m ensurepip
