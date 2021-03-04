SELECT parts.*, columns.compressed_size, columns.uncompressed_size
    FROM 
        (SELECT 
            table, 
            sum(data_uncompressed_bytes) AS uncompressed_size, 
            sum(data_compressed_bytes) AS compressed_size
        FROM system.columns 
        WHERE database = '${db}' AND table = '${table}'
        GROUP BY table
    ) AS columns 
    RIGHT JOIN 
    (SELECT
            table, 
            sum(rows) AS rows, 
            max(modification_time) AS last_modified, 
            sum(bytes) AS disk_size, 
            sum(primary_key_bytes_in_memory) AS pk_size, 
            any(engine) AS engine, 
            sum(bytes) AS bytes_size
        FROM system.parts 
        WHERE active AND database = '{db}' AND table = '{table}'
        GROUP BY database, table
    ) AS parts ON columns.table = parts.table