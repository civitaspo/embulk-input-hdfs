package org.embulk.input.hdfs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class TargetFileInfo
    implements Serializable
{
    // private static final long serialVersionUID = 1L; // to suppress warnings?
    private long start;
    private long end;
    private String pathString;
    private boolean isDecompressible;
    private boolean isPartitionable;

    @JsonCreator
    public TargetFileInfo(
            @JsonProperty("path_string") String pathString,
            @JsonProperty("start") long start,
            @JsonProperty("end") long end,
            @JsonProperty("is_decompressible") boolean isDecompressible,
            @JsonProperty("is_partitionable") boolean isPartitionable)
    {
        this.pathString = pathString;
        this.start = start;
        this.end = end;
        this.isDecompressible = isDecompressible;
        this.isPartitionable = isPartitionable;
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

    @JsonIgnore
    public long getSize()
    {
        return getEnd() - getStart();
    }
}
