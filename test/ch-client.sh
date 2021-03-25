#!/bin/sh
docker run -it --network="db-network" yandex/clickhouse-client -h clickhouse-server
