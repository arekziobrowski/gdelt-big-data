# Change the image id to the quickstart
FROM cloudera_base
COPY setup-python.sh /tmp/setup-python.sh
RUN /tmp/setup-python.sh

# Install Python packages
RUN python2.7 -m pip install redis
RUN python2.7 -m pip install matplotlib
RUN python2.7 -m pip install hdfs

# Create HDFS structure
COPY hdfs/initialize_tech.sh /tmp/initialize_tech.sh
RUN echo '/tmp/initialize_tech.sh' >> '/etc/profile'
