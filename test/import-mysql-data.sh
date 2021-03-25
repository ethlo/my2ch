#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
mkdir -p "$DIR/tmp" && cd "$_"
FILENAME="test_db.tar.gz"
if [ ! -f "$FILENAME" ]; then
    echo "Downloading test data"
    wget "https://github.com/datacharmer/test_db/releases/download/v1.0.7/test_db-1.0.7.tar.gz" -O "$FILENAME" || exit 2
fi
tar xzf "$FILENAME"
cd test_db

mysql -h127.0.0.1 -uroot -pexample < employees.sql

#echo 'Importing'
#docker run --rm --network="db-network" imega/mysql-client mysql:10.5.6 --host=mysql-server --user=root --password=example
