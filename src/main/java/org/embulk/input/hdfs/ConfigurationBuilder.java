package org.embulk.input.hdfs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.embulk.config.ConfigException;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.io.File;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;

/**
 * Created by takahiro.nakayama on 2/22/16.
 */
public class ConfigurationBuilder
{
    private static final Logger logger = Exec.getLogger(ConfigurationBuilder.class);
    private final ImmutableList.Builder<String> configFilesBuilder;
    private final ImmutableMap.Builder<String, String> configMapBuilder;

    public ConfigurationBuilder()
    {
        this.configFilesBuilder = ImmutableList.builder();
        this.configMapBuilder = ImmutableMap.builder();
    }

    public ConfigurationBuilder addConfigFiles(List<String> configFiles)
    {
        for (String configFile : configFiles) {
            addConfigFile(configFile);
        }
        return this;
    }

    public ConfigurationBuilder addConfigFile(String configFile)
    {
        configFilesBuilder.add(configFile);
        return this;
    }

    public ConfigurationBuilder addConfigMap(Map<String, String> configMap)
    {
        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            addConfig(entry.getKey(), entry.getValue());
        }
        return this;
    }

    public ConfigurationBuilder addConfig(String key, String value)
    {
        configMapBuilder.put(key, value);
        return this;
    }

    public Configuration build()
    {
        Configuration configuration = new Configuration();
        for (String configFile : configFilesBuilder.build()) {
            File file = new File(configFile);
            try {
                configuration.addResource(file.toURI().toURL());
            }
            catch (MalformedURLException e) {
                throw new ConfigException(e);
            }
        }
        for (Map.Entry<String, String> entry : configMapBuilder.build().entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }
        // For debug
        for (Map.Entry<String, String> entry : configuration) {
            logger.trace("{}: {}", entry.getKey(), entry.getValue());
        }
        logger.trace("Resource Files: {}", configuration);
        return configuration;
    }
}
