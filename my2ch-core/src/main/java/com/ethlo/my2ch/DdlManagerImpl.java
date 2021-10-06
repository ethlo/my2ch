package com.ethlo.my2ch;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
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
import com.ethlo.my2ch.config.ClickHouseConfig;
import com.ethlo.my2ch.config.Ddl;
import com.ethlo.my2ch.config.LifeCycle;
import com.ethlo.my2ch.config.TransferConfig;
import com.ethlo.time.Chronograph;

@Service
public class DdlManagerImpl implements DdlManager
{
    private static final Logger logger = LoggerFactory.getLogger(DdlManagerImpl.class);

    public static final String MIGRATIONS_PATH_NAME = "migrations";

    @Override
    public void run(final Path home, final String alias, LifeCycle lifeCycle)
    {
        final Path migrationsForAliasPath = home.resolve(DdlManagerImpl.MIGRATIONS_PATH_NAME).resolve(alias);
        if (!Files.exists(migrationsForAliasPath))
        {
            return;
        }
        logger.info("Looking for migrations related to alias {} in path {}", alias, migrationsForAliasPath);
        final TransferConfig config = My2chConfigLoader.loadConfig(home, alias, TransferConfig.class);
        final List<Ddl> ddls = My2chConfigLoader.getDDLs(migrationsForAliasPath);
        final ClickHouseConfig clickhouseCfg = config.getTarget().getClickhouse();
        final ClackShack clackShack = new ClackShackImpl("http://" + clickhouseCfg.getHost() + ":" + clickhouseCfg.getPort());
        for (final Ddl ddl : ddls)
        {
            if (ddl.getLifecycle() == lifeCycle)
            {
                runQuery(clackShack, "CREATE TABLE IF NOT EXISTS my2ch_migrations (alias String, version String, timestamp DateTime, lifecycle Enum('before'=1, 'after'=2), result Enum('success'=1, 'failure'=2), elapsed String, checksum String) " +
                        "ENGINE = MergeTree " +
                        "ORDER BY (alias, version)");

                final Map<String, Object> params = new LinkedHashMap<>();
                params.put("alias", alias);
                params.put("version", ddl.getVersion());
                params.put("lifecycle", ddl.getLifecycle().name().toLowerCase());
                params.put("result", "failure");
                params.put("timestamp", LocalDateTime.now());
                params.put("checksum", checksum(ddl.getQuery()));

                // TODO: Consider failing if already run and failure

                // TODO: Consider using hash of DDL query to detect changes in definitions files VS what has been applied

                if (checkNotRunOrSuccessful(clackShack, params))
                {
                    logMigrationFailure(clackShack, params);
                    final Chronograph chronograph = Chronograph.create();
                    chronograph.start("ddl");
                    logger.info("Running DDL with version {} for alias {} at lifecycle {}", ddl.getVersion(), alias, ddl.getLifecycle());
                    runQuery(clackShack, ddl.getQuery());
                    chronograph.stop();
                    params.put("elapsed", chronograph.getTotalTime().toString());
                    logMigrationSuccess(clackShack, params);
                }
            }
        }
    }

    private void runQuery(ClackShack clackShack, String s)
    {
        clackShack.ddl(s)
                .join();
    }

    private String checksum(String query)
    {
        final Checksum crc32 = new CRC32();
        crc32.update(query.getBytes(StandardCharsets.UTF_8));
        return Long.toString(crc32.getValue());
    }

    private boolean checkNotRunOrSuccessful(final ClackShack clackShack, Map<String, Object> params)
    {
        final ResultSet result = clackShack.query("SELECT * FROM my2ch_migrations WHERE alias = :alias AND version = :version", params).join();
        if (result.isEmpty())
        {
            return true;
        }

        final boolean wasSuccessful = "success".equals(result.getRow(0).get("result"));
        if (!wasSuccessful)
        {
            throw new IllegalStateException(String.format("Found a failed migration for %s, version %s", params.get("alias"), params.get("version")));
        }
        return true;
    }

    private void logMigrationSuccess(final ClackShack clackShack, final Map<String, Object> params)
    {
        clackShack.insert("ALTER TABLE my2ch_migrations UPDATE result = 'success' WHERE alias = :alias AND version = :version", params).join();
    }

    private void logMigrationFailure(ClackShack clackShack, Map<String, Object> params)
    {
        clackShack.insert("INSERT INTO my2ch_migrations VALUES (:alias, :version, :timestamp, :lifecycle, :result, :elapsed, :checksum)", params).join();
    }
}
