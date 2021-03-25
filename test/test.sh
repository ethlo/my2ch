#!/bin/bash
set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
docker-compose up -d
$DIR/import-mysql-data.sh
$DIR/my2ch.sh
$DIR/ch-client.sh
