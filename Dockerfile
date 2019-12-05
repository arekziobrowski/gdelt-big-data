# Change the image id to the quickstart
FROM d930858b09fd3962490e0e8a3ed3d3ed8f65cc6a71212c89dd327ca84b639024
COPY setup-python.sh /tmp/setup-python.sh
RUN /tmp/setup-python.sh
