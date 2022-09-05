package com.ethlo.my2ch;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ethlo.clackshack.ClackShack;
import com.ethlo.clackshack.ClackShackImpl;
import com.ethlo.clackshack.model.ResultSet;
import com.ethlo.clackshack.model.Row;
import com.ethlo.my2ch.config.ClickHouseConfig;
import com.ethlo.my2ch.config.Ddl;
import com.ethlo.my2ch.config.LifeCycle;
import com.ethlo.my2ch.config.TransferConfig;
import com.ethlo.time.Chronograph;

@Service
public class DdlManagerImpl implements DdlManager
{
    public static final String MIGRATIONS_PATH_NAME = "migrations";
    private static final Logger logger = LoggerFactory.getLogger(DdlManagerImpl.class);

    @Override
    public void run(final Path dir, final LifeCycle lifeCycle)
    {
        final Path migrationsForAliasPath = dir.resolve(DdlManagerImpl.MIGRATIONS_PATH_NAME);
        if (!Files.exists(migrationsForAliasPath))
        {
            return;
        }
        final String alias = My2chConfigLoader.getAlias(dir);
        logger.debug("Looking for migrations related to alias {} in path {}", alias, migrationsForAliasPath);
        final TransferConfig config = My2chConfigLoader.loadConfig(dir.resolve("transfer.yml"), TransferConfig.class);

        final List<Ddl> ddls = My2chConfigLoader.getDDLs(migrationsForAliasPath);
        final ClickHouseConfig clickhouseCfg = config.getTarget().getClickhouse();
        final ClackShack clackShack = new ClackShackImpl(clickhouseCfg.getUrl());
        for (final Ddl ddl : ddls)
        {
            if (ddl.getLifecycle() == lifeCycle)
            {
                runQuery(clackShack, "CREATE TABLE IF NOT EXISTS my2ch_migrations (alias String, version String, timestamp DateTime64(3), lifecycle Enum('before'=1, 'after'=2), status Enum('start'=0, 'success'=1, 'failure'=2), elapsed String, checksum String) " +
                        "ENGINE = MergeTree " +
                        "ORDER BY (alias, version)");

                final Map<String, Object> params = new LinkedHashMap<>();
                params.put("alias", alias);
                params.put("version", ddl.getVersion());
                params.put("lifecycle", ddl.getLifecycle().name().toLowerCase());
                params.put("status", "start");
                params.put("elapsed", "0");
                params.put("timestamp", nowDateString());
                params.put("checksum", checksum(ddl.getQuery()));

                if (checkNotRunOrSuccessful(clackShack, params))
                {
                    logMigration(clackShack, params);
                    final Chronograph chronograph = Chronograph.create();
                    chronograph.start("ddl");
                    logger.info("Running DDL with version {} for alias {} at lifecycle {}", ddl.getVersion(), alias, ddl.getLifecycle());

                    runQuery(clackShack, ddl.getQuery());
                    chronograph.stop();

                    params.put("status", "success");
                    params.put("timestamp", nowDateString());
                    params.put("elapsed", chronograph.getTotalTime().toString());
                    logMigration(clackShack, params);
                }
            }
        }
    }

    private String nowDateString()
    {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
    }

    private void runQuery(ClackShack clackShack, String s)
    {
        clackShack.ddl(s);
    }

    private String checksum(String query)
    {
        final Checksum crc32 = new CRC32();
        crc32.update(query.getBytes(StandardCharsets.UTF_8));
        return Long.toString(crc32.getValue());
    }

    private boolean checkNotRunOrSuccessful(final ClackShack clackShack, Map<String, Object> params)
    {
        final ResultSet result = clackShack.query("SELECT * FROM my2ch_migrations WHERE alias = :alias AND version = :version order by timestamp DESC", params);
        if (result.isEmpty())
        {
            return true;
        }

        final Row row = result.getRow(0);
        final boolean wasSuccessful = "success".equals(row.get("status"));
        if (!wasSuccessful)
        {
            throw new IllegalStateException(String.format("Found a failed migration for %s, version %s", params.get("alias"), params.get("version")));
        }

        // TODO: Check checksum match!

        return false;
    }

    private void logMigration(ClackShack clackShack, Map<String, Object> params)
    {
        clackShack.insert("INSERT INTO my2ch_migrations VALUES (:alias, :version, :timestamp, :lifecycle, :status, :elapsed, :checksum)", params);
    }
}
