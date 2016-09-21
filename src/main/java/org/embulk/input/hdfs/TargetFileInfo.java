package org.embulk.input.hdfs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class TargetFileInfo
    implements Serializable
{
    // private static final long serialVersionUID = 1L; // to suppress warnings?
    private final long start;
    private final long end;
    private final String pathString;
    private final boolean isDecompressible;
    private final boolean isPartitionable;
    private final int numHeaderLines;

    @JsonCreator
    public TargetFileInfo(
            @JsonProperty("path_string") String pathString,
            @JsonProperty("start") long start,
            @JsonProperty("end") long end,
            @JsonProperty("is_decompressible") boolean isDecompressible,
            @JsonProperty("is_partitionable") boolean isPartitionable,
            @JsonProperty("num_header_lines") int numHeaderLines)
    {
        this.pathString = pathString;
        this.start = start;
        this.end = end;
        this.isDecompressible = isDecompressible;
        this.isPartitionable = isPartitionable;
        this.numHeaderLines = numHeaderLines;
    }

    @JsonProperty("start")
    public long getStart()
    {
        return start;
    }

    @JsonProperty("end")
    public long getEnd()
    {
        return end;
    }

    @JsonProperty("is_decompressible")
    public boolean getIsDecompressible()
    {
        return isDecompressible;
    }

    @JsonProperty("is_partitionable")
    public boolean getIsPartitionable()
    {
        return isPartitionable;
    }

    @JsonProperty("path_string")
    public String getPathString()
    {
        return pathString;
    }

    @JsonProperty("num_header_lines")
    public int getNumHeaderLines()
    {
        return numHeaderLines;
    }

    @JsonIgnore
    public long getSize()
    {
        // NOTE: this size is reference value which
        //       becomes smaller than raw if the file is compressed.
        return getEnd() - getStart();
    }
}
