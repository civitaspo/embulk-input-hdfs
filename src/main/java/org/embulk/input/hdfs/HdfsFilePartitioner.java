package org.embulk.input.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by takahiro.nakayama on 8/20/15.
 */
class HdfsFilePartitioner
{
    private FileSystem fs;
    private Path path;
    private int partitionNum;

    public HdfsFilePartitioner(FileSystem fs, Path path, int partitionNum)
    {
        this.fs = fs;
        this.path = path;
        this.partitionNum = partitionNum;
    }

    public List<HdfsPartialFile> getHdfsPartialFiles() throws IOException
    {
        List<HdfsPartialFile> hdfsPartialFiles = new ArrayList<>();
        long size = fs.getFileStatus(path).getLen();
        for (int i = 0; i < partitionNum; i++) {
            long start = size * i / partitionNum;
            long end = size * (i + 1) / partitionNum;
            if (start < end) {
                hdfsPartialFiles.add(new HdfsPartialFile(path, start, end));
            }
        }
        return hdfsPartialFiles;
    }
}
