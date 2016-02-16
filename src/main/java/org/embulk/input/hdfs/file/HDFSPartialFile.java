package org.embulk.input.hdfs.file;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;

/**
 * Created by takahiro.nakayama on 2/13/16.
 * ref. https://github.com/hito4t/embulk-input-filesplit/blob/master/src/main/java/org/embulk/input/filesplit/PartialFile.java
 */
public class HDFSPartialFile
{
    private final HDFSFile hdfsFile;
    private final long start;
    private final long end;

    public HDFSPartialFile(HDFSFile hdfsFile, long start, long end)
    {
        this.hdfsFile = hdfsFile;
        this.start = start;
        this.end = end;
    }

    public HDFSFile getHdfsFile()
    {
        return hdfsFile;
    }

    public long getStart()
    {
        return start;
    }

    public long getEnd()
    {
        return end;
    }

    public Path getPath()
    {
        return getHdfsFile().getPath();
    }

    public CompressionCodec getCodec()
    {
        return getHdfsFile().getCodec();
    }

    public InputStream createInputStream(FileSystem fs)
            throws IOException
    {
        InputStream original = getHdfsFile().createInputStream(fs);
        return new HDFSPartialFileInputStream(original, getStart(), getEnd());
    }

    public InputStream createHeadersInputStream(FileSystem fs, int numHeaderLines)
            throws IOException
    {
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        int skippedHeaders = 0;
        InputStream hdfsFileInputStream = createInputStream(fs);
        try (BufferedInputStream in = new BufferedInputStream(hdfsFileInputStream)) {
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

    public InputStream createInputStreamWithHeaders(FileSystem fs, int numHeaderLines)
            throws IOException
    {
        return new SequenceInputStream(createHeadersInputStream(fs, numHeaderLines), createInputStream(fs));
    }
}
