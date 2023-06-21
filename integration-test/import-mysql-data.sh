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

echo 'Importing into MySQL'
docker run -it --network=db-network --rm -v$PWD:/docker-entrypoint-initdb.d mysql mysql -hmysql-server -uroot -pexample
