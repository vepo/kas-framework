#!/bin/bash

docker exec kafka-0 apk add iproute2-tc
docker exec kafka-1 apk add iproute2-tc
docker exec kafka-2 apk add iproute2-tc
docker run --privileged -v /var/run/docker.sock:/var/run/docker.sock gaiaadm/pumba netem -d 25m delay --time 300 re2:^kafka
