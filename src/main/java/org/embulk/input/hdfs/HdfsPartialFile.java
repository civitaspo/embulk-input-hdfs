package org.embulk.input.hdfs;

import org.apache.hadoop.fs.Path;

/**
 * Created by takahiro.nakayama on 8/20/15.
 */
// ref. https://github.com/hito4t/embulk-input-filesplit/blob/master/src/main/java/org/embulk/input/filesplit/PartialFile.java
class HdfsPartialFile
{
    private Path path;
    private long start;
    private long end;

    public HdfsPartialFile(Path path, long start, long end)
    {
        this.path = path;
        this.start = start;
        this.end = end;
    }

    public Path getPath()
    {
        return path;
    }

    public long getStart()
    {
        return start;
    }

    public long getEnd()
    {
        return end;
    }

}
