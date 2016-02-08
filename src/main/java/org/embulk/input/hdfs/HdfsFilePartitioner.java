package org.embulk.input.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by takahiro.nakayama on 8/20/15.
 */
public class HdfsFilePartitioner
{
    private FileSystem fs;
    private Path path;
    private long numPartitions;
    private long rawLength;

    public HdfsFilePartitioner(FileSystem fs, Path path, long numPartitions, long rawLength)
    {
        this.fs = fs;
        this.path = path;
        this.numPartitions = numPartitions;
        this.rawLength = rawLength;
    }

    public List<HdfsPartialFile> getHdfsPartialFiles()
            throws IOException
    {
        List<HdfsPartialFile> hdfsPartialFiles = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            long start = rawLength * i / numPartitions;
            long end = rawLength * (i + 1) / numPartitions;
            if (start < end) {
                hdfsPartialFiles.add(new HdfsPartialFile(path.toString(), start, end));
            }
        }
        return hdfsPartialFiles;
    }
}
