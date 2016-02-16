package org.embulk.input.hdfs.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.embulk.input.hdfs.file.HDFSFile;

/**
 * Created by takahiro.nakayama on 2/15/16.
 */
public class HDFSFileFactory
{
    private final CompressionCodecFactory codecFactory;

    public HDFSFileFactory(Configuration configuration)
    {
        this.codecFactory = new CompressionCodecFactory(configuration);
    }

    private CompressionCodecFactory getCodecFactory()
    {
        return codecFactory;
    }

    public HDFSFile create(Path path)
    {
        return create(path, true);
    }

    public HDFSFile create(Path path, boolean decompression)
    {
        return new HDFSFile(path, getCodecFactory().getCodec(path), decompression);
    }
}
