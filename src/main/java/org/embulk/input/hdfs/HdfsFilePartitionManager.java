package org.embulk.input.hdfs;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by takahiro.nakayama on 8/20/15.
 */
public class HdfsFilePartitionManager
{
    private FileSystem fs;
    private List<Path> fileList;
    private int partitionSize;
    private final int partitionParameter = 2;

    public HdfsFilePartitionManager(FileSystem fs, List<String> fileList, int partitionSize)
    {
        this.fs = fs;
        this.fileList = Lists.transform(fileList, new Function<String, Path>() {
            @Nullable
            @Override
            public Path apply(String filePathString) {
                return new Path(filePathString);
            }
        });
        this.partitionSize = partitionSize;
    }

    public List<HdfsPartialFile> getHdfsPartialFiles() throws IOException
    {
        if (partitionSize <= 0) {
            long fileLengthSum = 0;
            for (Path path : fileList) {
                fileLengthSum += fs.getFileStatus(path).getLen();
            }
            partitionSize = (int) (
                    fileLengthSum / (Runtime.getRuntime().availableProcessors() * partitionParameter));
        }

        List<HdfsPartialFile> hdfsPartialFiles = new ArrayList<>();
        for (Path path : fileList) {
            int fileLength = (int) fs.getFileStatus(path).getLen();
            int partitionNum = fileLength / partitionSize;
            int remainder = fileLength % partitionNum;

            if (remainder > 0) {
                partitionNum++;
            }

            HdfsFilePartitioner partitioner = new HdfsFilePartitioner(fs, path, partitionNum);
            hdfsPartialFiles.addAll(partitioner.getHdfsPartialFiles());
        }

        return hdfsPartialFiles;
    }
}
