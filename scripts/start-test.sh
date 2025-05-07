#!/bin/bash -e

mvn clean package -DskipTests

rm -rf scripts/docker/experiment/runtime/*.jar

cp experiment/data-generator/target/data-generator-jar-with-dependencies.jar scripts/docker/experiment/runtime/
cp experiment/sample-stream/target/sample-stream-jar-with-dependencies.jar   scripts/docker/experiment/runtime/

(cd scripts/docker/experiment && docker-compose up -d)