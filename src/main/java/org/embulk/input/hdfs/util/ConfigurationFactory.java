package org.embulk.input.hdfs.util;

import org.apache.hadoop.conf.Configuration;
import org.embulk.config.ConfigException;
import org.embulk.input.hdfs.HdfsFileInputPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.io.File;
import java.net.MalformedURLException;
import java.util.Map;

/**
 * Created by takahiro.nakayama on 2/9/16.
 */

public class ConfigurationFactory
{
    private static final Logger logger = Exec.getLogger(ConfigurationFactory.class);

    private ConfigurationFactory()
    {
    }

    public static Configuration fromTask(PluginTask task)
    {
        Configuration configuration = newConfiguration();
        for (String configFile : task.getConfigFiles()) {
            File file = new File(configFile);
            try {
                configuration.addResource(file.toURI().toURL());
            }
            catch (MalformedURLException e) {
                throw new ConfigException(e);
            }
        }
        for (Map.Entry<String, String> entry : task.getConfig().entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }
        // For debug
        for (Map.Entry<String, String> entry : configuration) {
            logger.trace("{}: {}", entry.getKey(), entry.getValue());
        }
        logger.trace("Resource Files: {}", configuration);
        return configuration;

    }

    public static Configuration newConfiguration()
    {
        return new Configuration();
    }
}
