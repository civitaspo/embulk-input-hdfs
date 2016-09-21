package org.embulk.input.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;

public class TargetFileInputStreamFactory
{
    private static final Logger logger = Exec.getLogger(TargetFileInputStreamFactory.class);
    private final FileSystem fs;

    public TargetFileInputStreamFactory(FileSystem fs)
    {
        this.fs = fs;
    }

    public InputStream create(TargetFileInfo t)
            throws IOException
    {
        InputStream is = createSuitableInputStream(t);
        return createInputStreamWithHeaders(is, t);
    }

    private InputStream createSuitableInputStream(TargetFileInfo t)
            throws IOException
    {
        if (t.getIsDecompressible()) {
            logger.debug("embulk-input-hdfs: createDecompressedInputStream: {}", t.getPathString());
            return createDecompressedInputStream(t);
        }
        else if (t.getIsPartitionable()) {
            logger.debug("embulk-input-hdfs: createPartialInputStream: {}, start:{}, end:{}",
                    t.getPathString(), t.getStart(), t.getEnd());
            return createPartialInputStream(t);
        }
        else {
            logger.debug("embulk-input-hdfs: createOriginalInputStream: {}", t.getPathString());
            return createOriginalInputStream(t);
        }
    }

    private InputStream createInputStreamWithHeaders(InputStream original, TargetFileInfo t)
            throws IOException
    {
        if (t.getStart() > 0 && t.getNumHeaderLines() > 0) {
            logger.debug("embulk-input-hdfs: createInputStreamWithHeaders: {}", t.getPathString());
            InputStream headers = createHeadersInputStream(t);
            return new SequenceInputStream(headers, original);
        }
        else {
            return original;
        }
    }

    private InputStream createOriginalInputStream(TargetFileInfo t)
            throws IOException
    {
        return fs.open(new Path(t.getPathString()));
    }

    private InputStream createDecompressedInputStream(TargetFileInfo t)
            throws IOException
    {
        InputStream original = createOriginalInputStream(t);
        CompressionCodecFactory factory = new CompressionCodecFactory(fs.getConf());
        CompressionCodec codec = factory.getCodec(new Path(t.getPathString()));
        if (codec == null) {
            logger.debug("embulk-input-hdfs: CompressionCodec:　null: {}", t.getPathString());
            return original;
        }
        else {
            logger.debug("embulk-input-hdfs: CompressionCodec:　{}: {}", codec, t.getPathString());
            return codec.createInputStream(original);
        }
    }

    private InputStream createPartialInputStream(TargetFileInfo t)
            throws IOException
    {
        InputStream original = createOriginalInputStream(t);
        return new TargetFilePartialInputStream(original, t.getStart(), t.getEnd());
    }

    private InputStream createHeadersInputStream(TargetFileInfo t)
            throws IOException
    {
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        int skippedHeaders = 0;
        InputStream is = createOriginalInputStream(t);
        try (BufferedInputStream in = new BufferedInputStream(is)) {
            while (true) {
                int c = in.read();
                if (c < 0) {
                    break;
                }

                header.write(c);

                if (c == '\n') {
                    skippedHeaders++;
                }
                else if (c == '\r') {
                    int c2 = in.read();
                    if (c2 == '\n') {
                        header.write(c2);
                    }
                    skippedHeaders++;
                }

                if (skippedHeaders >= t.getNumHeaderLines()) {
                    break;
                }
            }
        }
        header.close();
        return new ByteArrayInputStream(header.toByteArray());
    }
}
