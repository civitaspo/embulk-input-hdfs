package org.embulk.input.hdfs.file;

import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by takahiro.nakayama on 2/13/16.
 */
public class HDFSFile
{
    private final Path path;
    private final CompressionCodec codec;
    private final boolean decompression;

    public HDFSFile(Path path, @Nullable CompressionCodec codec, boolean decompression)
    {
        this.path = path;
        this.codec = codec;
        this.decompression = decompression;
    }

    public Path getPath()
    {
        return path;
    }

    public CompressionCodec getCodec()
    {
        return codec;
    }

    public boolean getDecompression()
    {
        return decompression;
    }

    public boolean canDecompress()
    {
        return getCodec() != null && getDecompression();
    }

    public long getLength(FileSystem fs)
            throws IOException
    {
        if (getCodec() == null) {
            return fs.getFileStatus(getPath()).getLen();
        }
        else if (!decompression) {
            return fs.getFileStatus(getPath()).getLen();
        }
        else {
            long fileLength = 0;
            try (InputStream is = createInputStream(fs)) {
                while (is.read() > 0) {
                    fileLength++;
                }
            }
            return fileLength;
        }
    }

    public InputStream createInputStream(FileSystem fs)
            throws IOException
    {
        if (getCodec() == null) {
            return fs.open(getPath());
        }
        else if (!getDecompression()) {
            return fs.open(getPath());
        }
        else {
            Decompressor decompressor = CodecPool.getDecompressor(getCodec());
            try {
                return getCodec().createInputStream(fs.open(getPath()), decompressor);
            }
            finally {
                CodecPool.returnDecompressor(decompressor);
            }
        }
    }
}
