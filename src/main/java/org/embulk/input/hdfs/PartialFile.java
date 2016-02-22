package org.embulk.input.hdfs;

import org.apache.hadoop.fs.Path;

/**
 * Created by takahiro.nakayama on 2/20/16.
 * is the same as PartialFileList.Entry, so this class does not need?
 */
public class PartialFile
{
    private final Path path;
    private final long start;
    private final long end;
    private final boolean canDecompress;

    public PartialFile(String path, long start, long end, boolean canDecompress)
    {
        this(new Path(path), start, end, canDecompress);
    }

    public PartialFile(Path path, long start, long end, boolean canDecompress)
    {
        this.path = path;
        this.start = start;
        this.end = end;
        this.canDecompress = canDecompress;
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

    public boolean getCanDecompress()
    {
        return canDecompress;
    }
}
