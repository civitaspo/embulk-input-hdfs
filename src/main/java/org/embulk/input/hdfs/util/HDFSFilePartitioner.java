package org.embulk.input.hdfs.util;

import com.google.common.collect.ImmutableList;
import org.embulk.input.hdfs.file.HDFSFile;
import org.embulk.input.hdfs.file.HDFSPartialFile;

import java.util.List;

/**
 * Created by takahiro.nakayama on 2/15/16.
 */
public class HDFSFilePartitioner
{
    private final HDFSFile hdfsFile;
    private final long fileLength;
    private final long numPartitions;

    public HDFSFilePartitioner(HDFSFile hdfsFile, long fileLength, long numPartitions)
    {
        this.hdfsFile = hdfsFile;
        this.fileLength = fileLength;
        this.numPartitions = numPartitions;
    }

    public List<HDFSPartialFile> generateHDFSPartialFiles()
    {
        ImmutableList.Builder<HDFSPartialFile> builder = ImmutableList.builder();
        for (long i = 0; i < numPartitions; i++) {
            long start = fileLength * i / numPartitions;
            long end = fileLength * (i + 1) / numPartitions;
            if (start < end) {
                builder.add(new HDFSPartialFile(hdfsFile, start, end));
            }
        }
        return builder.build();
    }
}
