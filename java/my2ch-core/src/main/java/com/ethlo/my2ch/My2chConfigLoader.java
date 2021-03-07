package com.ethlo.my2ch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;

import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationProperty;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.origin.PropertySourceOrigin;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.util.StringUtils;

import com.ethlo.my2ch.config.TransferConfig;

public class My2chConfigLoader
{
    private static final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    public static TransferConfig loadConfig(final Path baseDir, final String alias)
    {
        try
        {
            return doLoadConfig(baseDir, alias);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private static TransferConfig doLoadConfig(final Path baseDir, final String alias) throws IOException
    {
        final FileSystemResource baseConfigResource = new FileSystemResource(baseDir.resolve("base.yml"));
        final FileSystemResource ymlConfigResource = new FileSystemResource(baseDir.resolve(alias + ".yml"));
        final FileSystemResource yamlConfigResource = new FileSystemResource(baseDir.resolve(alias + ".yaml"));
        final FileSystemResource configResource = ymlConfigResource.exists() ? ymlConfigResource : yamlConfigResource;

        final List<PropertySource<?>> propertySources = new LinkedList<>();
        if (baseConfigResource.exists())
        {
            propertySources.addAll(new YamlPropertySourceLoader().load("base", baseConfigResource));
        }

        propertySources.addAll(new YamlPropertySourceLoader().load(alias, configResource));
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
        final TransferConfig config = binder.bind(ConfigurationPropertyName.of(""), Bindable.of(TransferConfig.class), new BindHandler()
        {
        }).get();
        final Set<ConstraintViolation<TransferConfig>> violations = validator.validate(config);
        if (!violations.isEmpty())
        {
            throw new IllegalArgumentException("Error loading " + configResource.getURI()
                    + ": " + StringUtils.collectionToDelimitedString(violations.stream().map(v -> v.getPropertyPath() + ": " + v.getMessage()).collect(Collectors.toList()), "\n"));
        }

        return config;
    }

    public static List<TransferConfig> loadConfigs(final Path basePath)
    {
        return getConfigFiles(basePath).stream().map(configFile ->
                My2chConfigLoader.loadConfig(basePath, getBaseName(configFile.getFileName().toString())))
                .collect(Collectors.toList());
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

    public static List<String> getConfigs(final Path basePath)
    {
        return getConfigFiles(basePath).stream().map(p -> getBaseName(p.getFileName().toString())).collect(Collectors.toList());
    }

    private static List<Path> getConfigFiles(final Path basePath)
    {
        try (final Stream<Path> st = Files.list(basePath))
        {
            return st
                    .filter(p ->
                    {
                        final String filename = p.getFileName().toString();
                        return (filename.endsWith(".yml") || filename.endsWith(".yaml")) && !getBaseName(filename).equals("base");
                    })
                    .collect(Collectors.toList());
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }
}
