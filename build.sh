#!/bin/sh
set -e

mvn clean install -DskipTests

cd my2ch-cli || exit
mvn -DskipTests spring-boot:build-image
#move
cd ..

