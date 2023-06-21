#!/bin/bash
set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
docker run --name my2ch --rm --network="db-network" -v"$DIR/configs":/data ethlocom/my2ch:0.1.0-SNAPSHOT --home=/data
