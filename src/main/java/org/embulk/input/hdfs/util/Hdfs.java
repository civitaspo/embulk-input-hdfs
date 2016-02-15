package org.embulk.input.hdfs.util;

import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.embulk.config.ConfigException;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;

import static org.embulk.input.hdfs.HdfsFileInputPlugin.PluginTask;

/**
 * Created by takahiro.nakayama on 2/8/16.
 */
public class Hdfs
{
    private final Logger logger = Exec.getLogger(Hdfs.class);
    private final PluginTask task;
    private final Configuration configuration;
    private FileSystem fs;
    private CompressionCodecFactory compressionCodecFactory;

    public Hdfs(PluginTask task)
    {
        this.task = task;
        this.configuration = newConfiguration(task);
    }

    public Configuration getConfiguration()
    {
        return configuration;
    }

    public Configuration newConfiguration(PluginTask task)
    {
        Configuration configuration = new Configuration();
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

    public FileSystem newFS(Configuration configuration)
    {
        try {
            fs = FileSystem.get(configuration);
            return fs;
        }
        catch (IOException e) {
            throw new ConfigException(e);
        }
    }

    public FileSystem getFS()
    {
        if (fs == null) {
            return newFS(getConfiguration());
        }
        return fs;
    }

    public CompressionCodecFactory newCompressionCodecFactory(Configuration configuration)
    {
        compressionCodecFactory = new CompressionCodecFactory(configuration);
        return compressionCodecFactory;
    }

    public CompressionCodecFactory getCompressionCodecFactory()
    {
        return newCompressionCodecFactory(getConfiguration());
    }

    public Optional<CompressionCodec> getCompressionCodec(CompressionCodecFactory compressionCodecFactory, Path path)
    {
        return Optional.fromNullable(compressionCodecFactory.getCodec(path));
    }

    public Optional<CompressionCodec> getCompressionCodec(Path path)
    {
        return getCompressionCodec(getCompressionCodecFactory(), path);
    }
}
