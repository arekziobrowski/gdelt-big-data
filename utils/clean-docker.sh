#!/bin/bash

docker stop $(docker ps -a -q)


for x in `docker ps -a | tail -n +2 | awk '{ print $1 }'`
do
    docker rm $x
done

docker volume prune --force
docker network prune --force
