package com.ethlo.my2ch;

/*-
 * #%L
 * Simple MySQL to ClickHouse ETL - Core
 * %%
 * Copyright (C) 2018 - 2022 Morten Haraldsen (ethlo)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationProperty;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.origin.PropertySourceOrigin;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.PropertyPlaceholderHelper;
import org.springframework.util.StringUtils;

import com.ethlo.my2ch.config.Ddl;

public class My2chConfigLoader
{
    private static final Logger logger = LoggerFactory.getLogger(My2chConfigLoader.class);

    private static final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    public static <T> T loadConfig(final Path path, final Class<T> type)
    {
        try
        {
            return doLoadConfig(path, type);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private static <T> T doLoadConfig(final Path path, Class<T> type) throws IOException
    {
        final FileSystemResource baseConfigResource = new FileSystemResource(path.getParent().getParent().resolve("_transfer.yml"));
        final FileSystemResource configResource = new FileSystemResource(path);

        final List<PropertySource<?>> propertySources = new LinkedList<>();

        // System environment
        final Map<String, String> env = System.getenv();
        propertySources.add(new SystemEnvironmentPropertySource("env", (Map) env));

        // Config file
        final String configName = path.getParent().getFileName().toString();
        propertySources.addAll(new YamlPropertySourceLoader().load(configName, replaceEnv(configResource, env)));

        // Base config file
        if (baseConfigResource.exists())
        {
            final String baseName = baseConfigResource.getFile().getAbsolutePath();
            logger.debug("Using {} as fallback for {}", baseName, path);
            propertySources.addAll(new YamlPropertySourceLoader().load(baseName, replaceEnv(baseConfigResource, env)));
        }

        final ConfigurationPropertySource s = name ->
        {
            if (name == null)
            {
                return null;
            }
            for (PropertySource<?> configurationPropertySource : propertySources)
            {
                Object value = configurationPropertySource.getProperty(name.toString());
                if (value != null)
                {
                    return new ConfigurationProperty(name, value, new PropertySourceOrigin(configurationPropertySource, configurationPropertySource.getName()));
                }
            }
            return null;
        };
        final Binder binder = new Binder(s);
        final T config = binder.bind(ConfigurationPropertyName.of(""), Bindable.of(type), new BindHandler()
        {
        }).get();
        final Set<ConstraintViolation<T>> violations = validator.validate(config);
        if (!violations.isEmpty())
        {
            throw new IllegalArgumentException("Error loading " + configResource.getURI()
                    + ": " + StringUtils.collectionToDelimitedString(violations.stream().map(v -> v.getPropertyPath() + ": " + v.getMessage()).collect(Collectors.toList()), "\n"));
        }

        return config;
    }

    private static Resource replaceEnv(final Resource resource, final Map<String, String> env)
    {
        try
        {
            final String raw = Files.readString(resource.getFile().toPath());
            final String replaced = new PropertyPlaceholderHelper("{{", "}}", null, false).replacePlaceholders(raw, env::get);
            return new ByteArrayResource(replaced.getBytes(StandardCharsets.UTF_8), resource.getDescription());
        }
        catch (IOException exc)
        {
            throw new UncheckedIOException(exc);
        }
    }

    private static String getBaseName(final String filename)
    {
        final int idx = filename.lastIndexOf(".");
        if (idx != -1)
        {
            return filename.substring(0, idx);
        }
        return filename;
    }

    public static List<Path> getConfigDirectories(final Path basePath, final List<String> includes)
    {
        try (final Stream<Path> st = Files.list(basePath))
        {
            return st
                    .filter(Files::isDirectory)
                    .filter(p -> {
                        try
                        {
                            return !Files.isHidden(p);
                        }
                        catch (IOException e)
                        {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .filter(p -> includes == null || includes.contains(p.getFileName().toString()))
                    .collect(Collectors.toList());
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    public static List<Ddl> getDDLs(final Path ddlPath)
    {
        try (final Stream<Path> st = Files.list(ddlPath))
        {
            return new ArrayList<>(st
                    .filter(p ->
                    {
                        final String filename = p.getFileName().toString();
                        return filename.endsWith(".yml");
                    })
                    .map(p -> My2chConfigLoader.loadConfig(p, Ddl.class))
                    .collect(Collectors.toMap(Ddl::getVersion, c -> c))
                    .values());
        }
        catch (FileNotFoundException ignored)
        {
            return Collections.emptyList();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private static String extractVersion(String fileName)
    {
        return null;
    }

    public static String getAlias(Path dir)
    {
        return dir.getFileName().toString();
    }
}
