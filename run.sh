#!/bin/sh
docker run --name my2ch --rm -v$PWD/logs:/tmp/logs -v$PWD/configs:/tmp/configs -e TZ=UTC ethlocom/my2ch:latest --home=/tmp "$@"
