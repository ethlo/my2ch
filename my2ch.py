import argparse
import json
import logging as logger
import os
import signal
import sys
from datetime import timedelta, datetime
from time import time

import hiyapyco
import humanize
import mysql.connector
import sqlparse
from clickhouse_driver import Client


def convert_type(mysql_type):
    lower = mysql_type.lower()
    # Specification taken here https://clickhouse.tech/docs/en/engines/database-engines/mysql/
    is_unsigned = 'unsigned' in lower
    if 'tinyint' in lower:
        return ('U' if is_unsigned else '') + 'Int8'
    if 'smallint' in lower:
        return ('U' if is_unsigned else '') + 'Int16'
    if 'mediumint' in lower:
        return ('U' if is_unsigned else '') + 'Int32'
    if 'bigint' in lower:
        'Int64'
    if 'int' in lower:
        return ('U' if is_unsigned else '') + 'Int32'
    if 'float' in lower:
        return 'Float32'
    if 'double' in lower:
        return 'Float64'
    if 'datetime' in lower:
        return 'DateTime'
    if 'date' in lower:
        return 'DateTime'
    if 'timestamp' in lower:
        return 'DateTime'
    if 'binary' in lower:
        return 'FixedString'
    if 'bit' in lower or 'boolean' in lower:
        return 'UInt8'
    return 'String'


def nullable(param, is_nullable):
    return f'Nullable({param})' if is_nullable else param


def clickhouse_table_def(mysql_conn, query, engine_definition, ch_target_db, ch_table_name):
    with mysql_conn.cursor() as cursor:
        tmp_name = f'my2ch_{ch_table_name}_tmp'
        cursor.execute('CREATE TEMPORARY TABLE `' + tmp_name + '` ' + query + ' LIMIT 0')
        cursor.execute('DESC ' + tmp_name)

        s = f'CREATE TABLE {ch_target_db}.{ch_table_name} ('
        columns = []
        for row in cursor:
            is_nullable = row[2] == 'YES'
            data_type = nullable(convert_type(row[1]), is_nullable)
            columns.append(f'`{row[0]}` {data_type}')
        cursor.close()
        s += (",\n".join(columns))
        s += f') {engine_definition}'
        return s


def fetch_storage_stats(ch_client, ch_target_db, ch_table_name):
    query = (f"SELECT parts.*, columns.compressed_size, columns.uncompressed_size\n"
             f"FROM \n"
             f"    (SELECT \n"
             f"        table, \n"
             f"        sum(data_uncompressed_bytes) AS uncompressed_size, \n"
             f"        sum(data_compressed_bytes) AS compressed_size\n"
             f"    FROM system.columns \n"
             f"    WHERE database = '{ch_target_db}' AND table = '{ch_table_name}'\n"
             f"    GROUP BY table\n"
             f") AS columns \n"
             f"RIGHT JOIN \n"
             f"(SELECT "
             f"        table, \n"
             f"        sum(rows) AS rows, \n"
             f"        max(modification_time) AS last_modified, \n"
             f"        sum(bytes) AS disk_size, \n"
             f"        sum(primary_key_bytes_in_memory) AS pk_size, \n"
             f"        any(engine) AS engine, \n"
             f"        sum(bytes) AS bytes_size\n"
             f"    FROM system.parts \n"
             f"    WHERE active AND database = '{ch_target_db}' AND table = '{ch_table_name}'\n"
             f"    GROUP BY database, table\n"
             f") AS parts ON columns.table = parts.table\n")
    keys = ['table', 'rows', 'last_modified', 'disk_size', 'pk_size', 'engine', 'bytes', 'compressed', 'uncompressed']
    values = next(iter(ch_client.execute(query)), None)
    return dict(zip(keys, values)) if values is not None else None


def drop_view(mysql_conn, view_name):
    with mysql_conn.cursor() as cursor:
        cursor.execute(f'DROP VIEW IF EXISTS {view_name}')
        logger.debug(f'Dropped view {view_name}')


def ensure_view(mysql_conn, ch_table_name, query, range_clause):
    with mysql_conn.cursor() as cursor:
        view_name = f'tmp_my2ch_{ch_table_name}'
        view_def = f'CREATE OR REPLACE VIEW `{view_name}` AS {query}' + (' ' + range_clause if range_clause else '')
        logger.debug(f'Creating view {view_name}: {view_def}')
        cursor.execute(view_def)
        return view_name


def main(args):
    base_dir = args['home']
    names = args['names']

    if not names:
        # None defined, use all yaml files
        config_dir = os.path.join(base_dir, 'configs')
        any_processed = False
        for file in os.listdir(config_dir):
            if file.endswith(".yaml"):
                data_alias = os.path.splitext(file)[0]
                any_processed = True
                process_single(base_dir, data_alias)
        if not any_processed:
            print('No configurations found')
    else:
        # Run only defined names
        for data_alias in args['names']:
            process_single(base_dir, data_alias)


def format_ddl(table_def):
    return sqlparse.format(table_def.rstrip(';'), reindent=True, reindent_aligned=True, keyword_case='upper')


def process_single(base_dir, data_alias):
    # Configure logging
    for handler in logger.root.handlers[:]:
        logger.root.removeHandler(handler)

    logger.basicConfig(filename='logs/' + data_alias + '.log',
                       level=logger.DEBUG,
                       format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
                       datefmt='%Y-%m-%dT%H:%M:%S', )
    std_handler = logger.StreamHandler(sys.stdout)
    std_handler.setLevel(logger.INFO)
    logger.getLogger().addHandler(std_handler)

    # Load config
    base_config_path = os.path.join(base_dir, 'configs')
    if os.path.exists(base_config_path):
        settings = hiyapyco.load(os.path.join(base_config_path, 'config.yml'),
                                 os.path.join(base_config_path, data_alias + '.yaml'), method=hiyapyco.METHOD_MERGE)
    else:
        settings = hiyapyco.load(os.path.join(base_config_path, data_alias + '.yaml'))

    settings = json.loads(json.dumps(settings))

    # from pprint import pprint
    # pprint(settings)

    print('')
    logger.info(f"Processing data set '{data_alias}'")

    # Connect to MYSQL
    mysql_host = settings['mysql']['host']
    mysql_user = settings['mysql']['user']
    mysql_password = settings['mysql']['pass']
    mysql_db = settings['mysql']['db']
    mysql_conn = mysql.connector.connect(host=mysql_host,
                                         user=mysql_user,
                                         password=mysql_password,
                                         database=mysql_db)
    logger.info(f'Connected to MySQL host {mysql_host}')

    # Create a table definition for Clickhouse based on the query's names and data-types
    transfer = settings['transfer']
    query = transfer['query']
    range_clause_tpl = transfer.get('range_clause', None)
    primary_key = transfer.get('primary_key', None)

    ch_target_db = settings['clickhouse']['db']
    data_alias = settings['clickhouse']['table_name']
    engine_def = settings['clickhouse']['engine_definition']

    is_incremental = range_clause_tpl is not None

    # Ensure table exists
    clickhouse_url = settings['clickhouse']['url']
    logger.debug('Using Clickhouse URL %s', clickhouse_url)
    ch_client = Client.from_url(clickhouse_url)

    table_def = clickhouse_table_def(mysql_conn, query, engine_def, ch_target_db, data_alias)
    logger.debug(f'Clickhouse table definition: {table_def}')
    table_exists = ch_client.execute(f'EXISTS TABLE {data_alias}')[0][0] == 1
    if table_exists:
        # Compare the generated table to the spec to see if something has changed
        existing_table_def = ch_client.execute(f'SHOW CREATE TABLE {data_alias}')[0][0]
        if format_ddl(existing_table_def) != format_ddl(table_def):
            logger.warning(f"Clickhouse definition for table '{data_alias}' "
                           f"seem to be out of date. Please drop the table and let it rebuild if possible.")

    # Create MySQL engine in clickhouse as proxy to mysql data
    logger.debug('Ensuring clickhouse connection to MySQL is set up')
    create_mysql_engine = f"CREATE DATABASE IF NOT EXISTS mysql_{mysql_db} ENGINE = MySQL('{mysql_host}', '{mysql_db}', '{mysql_user}', '{mysql_password}')"
    logger.debug('Command to create MySQL DB proxy in Clickhouse: %s', create_mysql_engine)
    ch_client.execute(create_mysql_engine)

    if is_incremental and table_exists:
        # Find current max
        result = ch_client.execute(f"SELECT MAX({primary_key}) FROM {data_alias}")
        max_primary_key = result[0][0]
        logger.debug(
            f"Current max value of column '{primary_key}' in Clickhouse table '{data_alias}': {max_primary_key}")

        range_clause = range_clause_tpl.format(max_primary_key=max_primary_key) if range_clause_tpl is not None else ''
        view_name = ensure_view(mysql_conn, data_alias, query, range_clause)
    else:
        ch_client.execute(f'drop table if exists {data_alias}')
        logger.debug(f'Creating clickhouse table {data_alias}')
        ch_client.execute(table_def)
        view_name = ensure_view(mysql_conn, data_alias, query, None)

    # Insert data from above proxy into real clickhouse table
    transfer_data(mysql_db, data_alias, ch_client, f"SELECT * FROM {view_name}", ch_target_db)
    drop_view(mysql_conn, view_name)

    # Output clickhouse table stats
    storage_stats = fetch_storage_stats(ch_client, ch_target_db, data_alias)
    if storage_stats is not None:
        utc_datetime = storage_stats['last_modified']
        now_timestamp = time()
        offset = datetime.fromtimestamp(now_timestamp) - datetime.utcfromtimestamp(now_timestamp)
        local_time = utc_datetime + offset
        ratio = storage_stats['uncompressed'] / storage_stats['compressed'] if storage_stats['uncompressed'] > 0 and storage_stats['compressed'] > 0 else 0.0
        logger.info(f"=== Status ===\nTotal rows: {storage_stats['rows']:,}"
                    f"\nLast modified: {humanize.naturaldelta(local_time)} ago ({utc_datetime}Z)"
                    f"\nDisk usage: {humanize.naturalsize(storage_stats['disk_size'])}"
                    f"\nCompression ratio: {ratio:.2f}X"
                    f"\nStorage engine: {storage_stats['engine']}"
                    )
    else:
        logger.info('No existing data')


def transfer_data(mysql_db_name, ch_table_name, client, query, ch_target_db):
    started = time()
    logger.debug(f'Transferring data from MySQL query to Clickhouse table {ch_table_name}')
    client.execute(f"use mysql_{mysql_db_name}")
    transfer_query = f'insert into {ch_target_db}.{ch_table_name} {query}'
    logger.debug("Transfer query: %s", transfer_query)
    progress = client.execute_with_progress(transfer_query)
    last = 0
    for num_rows, total_rows in progress:
        if last != num_rows:
            logger.info(f'{num_rows:,} processed')
            last = num_rows

    elapsed = time() - started
    logger.info(f'Transferred {last:,} rows in %.3f seconds', timedelta(seconds=elapsed).total_seconds())

    return last


def handler():
    global c
    print("\nExiting...")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handler)

    parser = argparse.ArgumentParser(description='My2Ch - MySQL query to Clickhouse')
    parser.add_argument('--home', help='Home folder where configs and logs reside. Defaults to the current directory',
                        default='.')
    parser.add_argument('--names', nargs='+',
                        help='The aliases to process. Must match a basename of a yaml file in ./configs')
    parser.add_argument('--direct', help='Do not create view in MySQL, but query directly from Clickhouse',
                        default=False, type=bool)
    args, leftovers = parser.parse_known_args()
    main(vars(args))
