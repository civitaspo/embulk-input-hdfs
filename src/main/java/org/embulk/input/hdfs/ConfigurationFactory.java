package org.embulk.input.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.io.File;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;

public class ConfigurationFactory
{
    public static final Logger logger = Exec.getLogger(ConfigurationFactory.class);

    interface Task
    {
        @Config("config_files")
        @ConfigDefault("[]")
        List<String> getConfigFiles();

        @Config("config")
        @ConfigDefault("{}")
        Map<String, String> getConfig();
    }

    private ConfigurationFactory()
    {
    }

    public static Configuration create(Task task)
    {
        Configuration c = new Configuration();
        for (String f : task.getConfigFiles()) {
            try {
                logger.debug("embulk-input-hdfs: load a config file: {}", f);
                c.addResource(new File(f).toURI().toURL());
            }
            catch (MalformedURLException e) {
                throw new ConfigException(e);
            }
        }

        for (Map.Entry<String, String> entry : task.getConfig().entrySet()) {
            logger.debug("embulk-input-hdfs: load a config: {}:{}", entry.getKey(), entry.getValue());
            c.set(entry.getKey(), entry.getValue());
        }

        // For logging
        for (Map.Entry<String, String> entry : c) {
            logger.trace("embulk-input-hdfs: {}: {}", entry.getKey(), entry.getValue());
        }
        logger.trace("embulk-input-hdfs: Resource Files: {}", c);

        return c;
    }
}
