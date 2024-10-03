#!/bin/bash

    docker run -v /var/run/docker.sock:/var/run/docker.sock pumba pumba netem -d 25m delay --time 300 re2:^kafka