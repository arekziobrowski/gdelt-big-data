# Change the image id to the quickstart
FROM 7a08dc
COPY setup-python.sh /tmp/setup-python.sh
RUN /tmp/setup-python.sh

# Install Python packages
RUN python2.7 -m pip install redis
RUN python2.7 -m pip install matplotlib
