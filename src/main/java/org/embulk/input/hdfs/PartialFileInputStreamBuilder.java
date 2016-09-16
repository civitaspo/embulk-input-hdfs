package org.embulk.input.hdfs;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;

/**
 * Created by takahiro.nakayama on 2/21/16.
 */
public class PartialFileInputStreamBuilder
{
    private static final Logger logger = Exec.getLogger(PartialFileInputStreamBuilder.class);
    private final FileSystem fs;
    private final PartialFile partialFile;
    private int numHeaderLines = 0;

    public PartialFileInputStreamBuilder(FileSystem fs, PartialFile partialFile)
    {
        this.fs = fs;
        this.partialFile = partialFile;
    }

    public InputStream build()
            throws IOException
    {
        logger.trace("path: {}, start: {}, end: {}, num_header_lines: {}",
                partialFile.getPath(), partialFile.getStart(), partialFile.getEnd(), numHeaderLines);
        if (partialFile.getStart() > 0 && numHeaderLines > 0) {
            return new SequenceInputStream(createHeadersInputStream(), createPartialFileInputStream());
        }
        else {
            return createPartialFileInputStream();
        }
    }

    public PartialFileInputStreamBuilder withHeaders(int numHeaderLines)
    {
        this.numHeaderLines = numHeaderLines;
        return this;
    }

    private InputStream createOriginalFileWrappedInputStream()
    {
        InputStream original = createOriginalFileInputStream();
        CompressionCodec codec = new CompressionCodecFactory(fs.getConf()).getCodec(partialFile.getPath());
        if (partialFile.getCanDecompress() && codec != null) {
            try {
                return codec.createInputStream(original);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
        else {
            return original;
        }
    }

    private InputStream createOriginalFileInputStream()
    {
        try {
            return fs.open(partialFile.getPath());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private InputStream createPartialFileInputStream()
    {
        InputStream original = createOriginalFileWrappedInputStream();
        return new PartialFileInputStream(original, partialFile.getStart(), partialFile.getEnd());
    }

    private InputStream createHeadersInputStream()
            throws IOException
    {
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        int skippedHeaders = 0;
        InputStream original = createOriginalFileWrappedInputStream();
        try (BufferedInputStream in = new BufferedInputStream(original)) {
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

                if (skippedHeaders >= numHeaderLines) {
                    break;
                }
            }
        }
        header.close();
        return new ByteArrayInputStream(header.toByteArray());
    }
}
