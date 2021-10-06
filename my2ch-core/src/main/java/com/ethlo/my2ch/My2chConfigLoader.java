package com.ethlo.my2ch;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
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
import org.springframework.core.io.FileSystemResource;
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
        final Map env = System.getenv();
        propertySources.add(new SystemEnvironmentPropertySource("env", env));

        // Config file
        propertySources.addAll(new YamlPropertySourceLoader().load(path.getParent().getFileName().toString(), configResource));

        // Base config file
        if (baseConfigResource.exists())
        {
            logger.debug("Using {} as fallback for {}", baseConfigResource.getFile().getAbsolutePath(), path);
            propertySources.addAll(new YamlPropertySourceLoader().load("transfer.yml", baseConfigResource));
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
