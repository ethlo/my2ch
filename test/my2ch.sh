#!/bin/bash
set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
mkdir -p "$DIR/logs"
docker run --name my2ch --rm -v"$DIR/logs":/logs -v"$DIR/configs":/configs --network=test_db-network -e TZ=UTC ethlocom/my2ch:latest --home=/ "$@"
