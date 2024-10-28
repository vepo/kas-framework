#!/bin/bash

docker exec kafka-0 apk add iproute2-tc
docker exec kafka-1 apk add iproute2-tc
docker exec kafka-2 apk add iproute2-tc
docker run --privileged -v /var/run/docker.sock:/var/run/docker.sock gaiaadm/pumba --random netem -d 65m delay --time 30 --jitter 10 --distribution normal re2:^kafka &
