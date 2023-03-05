#!/usr/bin/env bash

set -ex

cd LakeSoul
mvn clean install -DskipTests -pl lakesoul-spark -am

cd ../ccf-bdci2022-datalake-contest
./build_lakesoul.sh

mv target/submit.zip ..

