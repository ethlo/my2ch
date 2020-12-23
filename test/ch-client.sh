#!/bin/sh
docker run -it --network test_db-network yandex/clickhouse-client -h clickhouse-server
