package org.embulk.input.hdfs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.embulk.config.ConfigException;

import java.io.Serializable;
import java.lang.reflect.Field;

public class TargetFileInfo
    implements Serializable
{
    public static class Builder
    {
        private Optional<Long> start = Optional.absent();
        private Optional<Long> end = Optional.absent();
        private Optional<String> pathString = Optional.absent();
        private Optional<Boolean> isDecompressible = Optional.absent();
        private Optional<Boolean> isPartitionable = Optional.absent();
        private Optional<Integer> numHeaderLines = Optional.absent();

        public Builder()
        {
        }

        public Builder start(long start)
        {
            this.start = Optional.of(start);
            return this;
        }

        public Builder end(long end)
        {
            this.end = Optional.of(end);
            return this;
        }

        public Builder pathString(String pathString)
        {
            this.pathString = Optional.of(pathString);
            return this;
        }

        public Builder isDecompressible(boolean isDecompressible)
        {
            this.isDecompressible = Optional.of(isDecompressible);
            return this;
        }

        public Builder isPartitionable(boolean isPartitionable)
        {
            this.isPartitionable = Optional.of(isPartitionable);
            return this;
        }

        public Builder numHeaderLines(int numHeaderLines)
        {
            this.numHeaderLines = Optional.of(numHeaderLines);
            return this;
        }

        public TargetFileInfo build()
        {
            try {
                validate();
            }
            catch (IllegalAccessException | IllegalStateException e) {
                throw new ConfigException(e);
            }

            return new TargetFileInfo(
                    pathString.get(), start.get(), end.get(),
                    isDecompressible.get(), isPartitionable.get(),
                    numHeaderLines.get());
        }

        private void validate()
                throws IllegalAccessException, IllegalStateException
        {
            for (Field field : getClass().getDeclaredFields()) {
                if (field.getType() != Optional.class) {
                    // for avoiding Z class by JUnit insertion.
                    continue;
                }
                Optional value = (Optional) field.get(this);
                if (!value.isPresent()) {
                    String msg = String.format("field:%s is absent", field.getName());
                    throw new IllegalStateException(msg);
                }
            }

            if (isDecompressible.get() && isPartitionable.get()) {
                String msg = String.format("IllegalState: isDecompressible is true and isPartitionable is true: %s", pathString.get());
                throw new IllegalStateException(msg);
            }

            if (isDecompressible.get() && start.get() != 0) {
                String msg = String.format("IllegalState: isDecompressible is true, but start is not 0: %s", pathString.get());
                throw new IllegalStateException(msg);
            }
        }
    }

    // private static final long serialVersionUID = 1L; // to suppress warnings?
    private final long start;
    private final long end;
    private final String pathString;
    private final boolean isDecompressible;
    private final boolean isPartitionable;
    private final int numHeaderLines;

    @JsonCreator
    private TargetFileInfo(
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
