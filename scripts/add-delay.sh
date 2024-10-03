#!/bin/bash

docker run --privileged -v /var/run/docker.sock:/var/run/docker.sock gaiaadm/pumba netem -d 25m delay --time 300 re2:^kafka
