# my2ch
Transfer a MySQL query result to a Clickhouse table with potential incremental updates.

## What is it?
A tool that allows you to define a MySQL query that is created as a table in ClickHouse for further analysis of the data. Schedule it to have daily updates or every minute to near realtime data.

## Why?
MySQL is a popular OLTP database, but it is not the best when it comes to analysis and statistics. Clickhouse is fantastic when it comes to richness  of query functions and raw speed of both queries and insert rate, which makes it a great match.

## How does it work?
This tool will automatically:
* Create a MySQL temporary table from query
* Describe the temp table to create a ClickHouse table representation
* Create a view in MySQL with the query
* Create a table in Clickhouse based on the definition 
* Create a MYSQL-engine database in ClickHouse to connect directly to MySQL
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
Create a folder called `configs/employees` and put the following `transfer.yaml` inside it, taking care to change the actual property values to match your setup. 
```yaml
alias: employees

target:
  primary-key: from_date
  table-name: employees
  engine-definition: ENGINE = MergeTree
      ORDER BY from_date
      SETTINGS index_granularity = 8192;

source:
  range-clause: where from_date > '{max_primary_key}'
  query: select e.emp_no, birth_date, s.from_date, s.to_date, s.salary
    from salaries s
    left join employees e on s.emp_no = e.emp_no

schedule:
  interval: PT30s
```

For reference, the data model in MySQL has the following structure:
![Schema](doc/schema.png)

#### Transfer data
Run the following command to transfer data:
```shell
docker run --name my2ch --rm -v $PWD/logs:/tmp/logs -v $PWD/configs:/tmp/configs -e TZ=UTC ethlocom/my2ch:latest
```

You should see something like the following:
```shell
Processing data set 'employees'
Connected to MySQL host 192.168.50.112
65,536 processed
131,072 processed
196,608 processed
...
2,686,976 processed
2,752,512 processed
2,844,047 processed
Transferred 2,844,047 rows in 5.847 seconds
=== Status ===
Total rows: 2,861,778
Last modified: a moment ago (2020-12-22 10:44:03Z)
Disk usage: 12.1 MB
Compression ratio: 5.24X
Storage engine: MergeTree

```

Running the same command again should not transfer any data (as there is no new data inserted in MySQL. This is controlled by the ` range_clause: and from_date > '{max_primary_key}'`).
```
Processing data set 'employees'
Connected to MySQL host 192.168.50.112
Transferred 0 rows in 0.104 seconds
=== Status ===
Total rows: 2,861,778
Last modified: 12 seconds ago (2020-12-22 10:44:03Z)
Disk usage: 12.1 MB
Compression ratio: 5.24X
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
│ 2002 │     158220 │
│ 2001 │     157821 │
│ 2000 │     155377 │
│ 1999 │     154885 │
│ 1998 │     151484 │
│ 1997 │     149675 │
│ 1996 │     149208 │
│ 1995 │     146531 │
│ 1994 │     143182 │
│ 1993 │     140625 │
└──────┴────────────┘

10 rows in set. Elapsed: 0.017 sec. Processed 2.86 million rows, 22.89 MB (163.83 million rows/s., 1.31 GB/s.)  
```
