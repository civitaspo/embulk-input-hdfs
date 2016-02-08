package org.embulk.input.hdfs;

/**
 * Created by takahiro.nakayama on 8/20/15.
 */
// ref. https://github.com/hito4t/embulk-input-filesplit/blob/master/src/main/java/org/embulk/input/filesplit/PartialFile.java
public class HdfsPartialFile
{
    private String path;
    private long start;
    private long end;

    public HdfsPartialFile(String path, long start, long end)
    {
        this.path = path;
        this.start = start;
        this.end = end;
    }

    // see: http://stackoverflow.com/questions/7625783/jsonmappingexception-no-suitable-constructor-found-for-type-simple-type-class
    public HdfsPartialFile()
    {
    }

    public String getPath()
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
