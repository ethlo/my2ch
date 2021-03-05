package my2ch;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationProperty;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.origin.PropertySourceOrigin;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.FileSystemResource;

import com.ethlo.my2ch.config.TransferConfig;

public class My2chConfigLoader
{
    public static TransferConfig loadConfig(final Path baseDir, final String alias) throws IOException
    {
        final FileSystemResource baseConfigResource = new FileSystemResource(baseDir.resolve("base.yml"));
        final FileSystemResource configResource = new FileSystemResource(baseDir.resolve(alias + ".yml"));

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
        return binder.bind("", ValidatedTransferConfig.class).get();
    }
}
