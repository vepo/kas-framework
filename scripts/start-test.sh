#!/bin/bash -e

./scripts/download-dataset

mvn package -DskipTests

(cd scripts/docker/experiment && docker-compose stop && docker-compose rm -f)

rm -rf scripts/docker/experiment/runtime/*.jar
rm -rf scripts/docker/experiment/dataset/*.json

cp experiment/data-generator/target/data-generator-jar-with-dependencies.jar scripts/docker/experiment/runtime/
cp experiment/sample-stream/target/sample-stream-jar-with-dependencies.jar   scripts/docker/experiment/runtime/

cp dataset/yellow_tripdata_2025-06.json                                      scripts/docker/experiment/dataset/data.json

(cd scripts/docker/experiment && docker-compose up -d)