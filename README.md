# my2ch
Transfer a MySQL query result to a Clickhouse table with potential incremental updates.

## What is it?
A simple command line tool that allows you to define a MySQL query that is created as a table in ClickHouse for further analysis of the data. Schedule it to have daily updates or every minute to near realtime data.

## Why?
MySQL is a popular OLTP database, but it is not the best when it comes to analysis and statistics. Clickhouse is fantastic when it comes to richness  of query functions and raw speed of both queries and insert rate, which makes it a great match.

## How does it work?

* Create a MySQL temp table from query
* Translate the table to ClickHouse format
* Create a view in MySQL with the query
* Create a table in Clickhouse with the definition
* Create a MYSQL-engine database in ClickHouse
* INSERT INTO SELECT from the view to the ClickHouse table

## Example

#### Prerequisites: 
* A running MySQL server
* Docker installed

For this example we are going to use the test data from the "employees" database, which can be found here: https://github.com/datacharmer/test_db/releases/download/v1.0.7/test_db-1.0.7.tar.gz

Decompress it and insert the data into your MYSQL server.

#### Starting a ClickHouse server
```shell
docker run -d --name clickhouse-server --ulimit nofile=262144:262144 -p 9000:9000 --volume=$HOME/clickhouse_database:/var/lib/clickhouse yandex/clickhouse-server
```

You can connect with the native ClickHouse client using th following command:
```shell
docker run -it --rm --link clickhouse-server:clickhouse-server yandex/clickhouse-client --host clickhouse-server
```


#### Configuration
Create a folder called `configs` and put the following `employees.yaml` inside it, taking care to change the actual property values to match your setup. 
```yaml
mysql:
  user: myuser
  pass: mypass
  db: employees
  host: 192.168.50.112

clickhouse:
  url: clickhouse://192.168.50.112
  db: default
  table_name: employees
  engine_definition: ENGINE = MergeTree
    ORDER BY from_date
    SETTINGS index_granularity = 8192;

transfer:
  primary_key: from_date
  range_clause: and from_date > '{max_primary_key}'
  query: select e.emp_no, birth_date, s.from_date, s.to_date, s.salary
    from salaries s
    left join employees e on s.emp_no = e.emp_no
    where birth_date > '1965-01-01'
```

#### Transfer data
Run the following command to transfer data:
```shell
docker run --name my2ch --rm -v $PWD/logs:/tmp/logs -v $PWD/configs:/tmp/configs -e TZ=UTC ethlocom/my2ch:latest
```

You should see something like the following:
```shell
Processing data set 'employees'
Connected to MySQL host 192.168.50.112
17,731 processed
Transferred 17,731 rows in 0.131 seconds
=== Status ===
Total rows: 17,731
Last modified: a moment ago (2020-12-22 10:04:23Z)
Disk usage: 151.2 kB
Compression ratio: 2.59X
Storage engine: MergeTree
```

Running the same command again should not transfer any data (as there is now new data). This is controlled by the `from_date` in the query:
```
Processing data set 'employees'
Connected to MySQL host 192.168.50.112
Transferred 0 rows in 0.104 seconds
=== Status ===
Total rows: 17,731
Last modified: 40 seconds ago (2020-12-22 10:04:23Z)
Disk usage: 151.2 kB
Compression ratio: 2.59X
Storage engine: MergeTree
```

#### Querying in ClickHouse
We can now test the data with a simple query:
```sql
SELECT
    toYear(from_date) AS year,
    max(salary) AS max_salary
FROM employees
GROUP BY year
ORDER BY max_salary DESC
LIMIT 10
```

```
┌─year─┬─max_salary─┐
│ 2001 │     130760 │
│ 2002 │     128403 │
│ 2000 │     127203 │
│ 1999 │     125542 │
│ 1993 │     125031 │
│ 1992 │     121590 │
│ 1998 │     121065 │
│ 1997 │     118777 │
│ 1996 │     117388 │
│ 1991 │     117257 │
└──────┴────────────┘

10 rows in set. Elapsed: 0.009 sec. Processed 17.73 thousand rows, 141.85 KB (2.05 million rows/s., 16.40 MB/s.) 
```