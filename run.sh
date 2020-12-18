#!/bin/sh
docker run --name my2ch --rm -v$PWD/logs:/logs -v$PWD/configs:/configs -e TZ=UTC ethlocom/my2ch:latest --home=/ "$@"
