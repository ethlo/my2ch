#!/bin/bash
mkdir -p tmp
cd tmp || exit 1
FILENAME="test_db.tar.gz"
if [ ! -f "$FILENAME" ]; then
    echo "Downloading test data"
    wget "https://github.com/datacharmer/test_db/releases/download/v1.0.7/test_db-1.0.7.tar.gz" -O "$FILENAME" || exit 2
fi
tar xzf "$FILENAME"
cd test_db
mysql -h127.0.0.1 -P33306 -uroot -pexample < employees.sql