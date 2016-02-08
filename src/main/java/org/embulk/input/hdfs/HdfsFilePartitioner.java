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

    public HdfsFilePartitioner(FileSystem fs, Path path, long numPartitions)
    {
        this.fs = fs;
        this.path = path;
        this.numPartitions = numPartitions;
    }

    public List<HdfsPartialFile> getHdfsPartialFiles()
            throws IOException
    {
        List<HdfsPartialFile> hdfsPartialFiles = new ArrayList<>();
        long size = fs.getFileStatus(path).getLen();
        for (int i = 0; i < numPartitions; i++) {
            long start = size * i / numPartitions;
            long end = size * (i + 1) / numPartitions;
            if (start < end) {
                hdfsPartialFiles.add(new HdfsPartialFile(path.toString(), start, end));
            }
        }
        return hdfsPartialFiles;
    }
}
