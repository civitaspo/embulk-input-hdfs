package org.embulk.input.hdfs;

// Ported from https://github.com/embulk/embulk-input-s3/blob/b546158123a734acf0785d61400c69fcdd910ed6/embulk-input-s3/src/main/java/org/embulk/input/s3/FileList.java
// and Modified for this package.

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.apache.commons.lang.SerializationUtils;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class TargetFileInfoList
{
    public interface Task
    {
        @Config("path_match_pattern")
        @ConfigDefault("\".*\"")
        String getPathMatchPattern();

        @Config("total_file_count_limit")
        @ConfigDefault("2147483647")
        int getTotalFileCountLimit();

        // TODO support more algorithms to combine tasks
        @Config("min_task_size")
        @ConfigDefault("0")
        long getMinTaskSize();
    }

    public static class Entry
    {
        private int index;
        private long size;

        @JsonCreator
        public Entry(
                @JsonProperty("index") int index,
                @JsonProperty("size") long size)
        {
            this.index = index;
            this.size = size;
        }

        @JsonProperty("index")
        public int getIndex()
        {
            return index;
        }

        @JsonProperty("size")
        public long getSize()
        {
            return size;
        }
    }

    public static class Builder
    {
        private final ByteArrayOutputStream binary;
        private final OutputStream stream;
        private final List<Entry> entries = new ArrayList<>();
        private TargetFileInfo last = null;

        private int limitCount = Integer.MAX_VALUE;
        private long minTaskSize = 0L;
        private Pattern pathMatchPattern = Pattern.compile(".*");

        private final ByteBuffer castBuffer = ByteBuffer.allocate(4);

        public Builder(Task task)
        {
            this();
            this.pathMatchPattern = Pattern.compile(task.getPathMatchPattern());
            this.limitCount = task.getTotalFileCountLimit();
            this.minTaskSize = task.getMinTaskSize();
        }

        public Builder(ConfigSource config)
        {
            this();
            this.pathMatchPattern = Pattern.compile(config.get(String.class, "path_match_pattern", ".*"));
            this.limitCount = config.get(int.class, "total_file_count_limit", Integer.MAX_VALUE);
            this.minTaskSize = config.get(long.class, "min_task_size", 0L);
        }

        public Builder()
        {
            binary = new ByteArrayOutputStream();
            try {
                stream = new BufferedOutputStream(new GZIPOutputStream(binary));
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        public Builder limitTotalFileCount(int limitCount)
        {
            this.limitCount = limitCount;
            return this;
        }

        public Builder minTaskSize(long bytes)
        {
            this.minTaskSize = bytes;
            return this;
        }

        public Builder pathMatchPattern(String pattern)
        {
            this.pathMatchPattern = Pattern.compile(pattern);
            return this;
        }

        public int size()
        {
            return entries.size();
        }

        public boolean needsMore()
        {
            return size() < limitCount;
        }

        // returns true if this file is used
        public synchronized boolean add(TargetFileInfo targetFileInfo, long size)
        {
            // TODO throw IllegalStateException if stream is already closed

            if (!needsMore()) {
                return false;
            }

            if (!pathMatchPattern.matcher(targetFileInfo.getPathString()).find()) {
                return false;
            }

            int index = entries.size();
            entries.add(new Entry(index, size));

            byte[] data = SerializationUtils.serialize(targetFileInfo);
            castBuffer.putInt(0, data.length);
            try {
                stream.write(castBuffer.array());
                stream.write(data);
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }

            last = targetFileInfo;
            return true;
        }

        public TargetFileInfoList build()
        {
            try {
                stream.close();
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
            return new TargetFileInfoList(binary.toByteArray(), getSplits(entries), Optional.fromNullable(last));
        }

        private List<List<Entry>> getSplits(List<Entry> all)
        {
            List<List<Entry>> tasks = new ArrayList<>();
            long currentTaskSize = 0;
            List<Entry> currentTask = new ArrayList<>();
            for (Entry entry : all) {
                currentTask.add(entry);
                currentTaskSize += entry.getSize();  // TODO consider to multiply the size by cost_per_byte, and add cost_per_file
                if (currentTaskSize >= minTaskSize) {
                    tasks.add(currentTask);
                    currentTask = new ArrayList<>();
                    currentTaskSize = 0;
                }
            }
            if (!currentTask.isEmpty()) {
                tasks.add(currentTask);
            }
            return tasks;
        }
    }

    private final byte[] data;
    private final List<List<Entry>> tasks;
    private final Optional<TargetFileInfo> last;

    @JsonCreator
    public TargetFileInfoList(
            @JsonProperty("data") byte[] data,
            @JsonProperty("tasks") List<List<Entry>> tasks,
            @JsonProperty("last") Optional<TargetFileInfo> last)
    {
        this.data = data;
        this.tasks = tasks;
        this.last = last;
    }

    @JsonIgnore
    public Optional<TargetFileInfo> getLastTargetFileInfo(Optional<TargetFileInfo> targetFileInfo)
    {
        if (last.isPresent()) {
            return last;
        }
        return targetFileInfo;
    }

    @JsonIgnore
    public int getTaskCount()
    {
        return tasks.size();
    }

    @JsonIgnore
    public List<TargetFileInfo> get(int i)
    {
        return new EntryList(data, tasks.get(i));
    }

    @JsonProperty("data")
    @Deprecated
    public byte[] getData()
    {
        return data;
    }

    @JsonProperty("tasks")
    @Deprecated
    public List<List<Entry>> getTasks()
    {
        return tasks;
    }

    @JsonProperty("last")
    @Deprecated
    public Optional<TargetFileInfo> getLast()
    {
        return last;
    }

    private class EntryList
            extends AbstractList<TargetFileInfo>
    {
        private final byte[] data;
        private final List<Entry> entries;
        private InputStream stream;
        private int current;

        private final ByteBuffer castBuffer = ByteBuffer.allocate(4);

        public EntryList(byte[] data, List<Entry> entries)
        {
            this.data = data;
            this.entries = entries;
            try {
                this.stream = new BufferedInputStream(new GZIPInputStream(new ByteArrayInputStream(data)));
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
            this.current = 0;
        }

        @Override
        public synchronized TargetFileInfo get(int i)
        {
            Entry e = entries.get(i);
            if (e.getIndex() < current) {
                // rewind to the head
                try {
                    stream.close();
                    stream = new BufferedInputStream(new GZIPInputStream(new ByteArrayInputStream(data)));
                }
                catch (IOException ex) {
                    throw Throwables.propagate(ex);
                }
                current = 0;
            }

            while (current < e.getIndex()) {
                readNext();
            }
            // now current == e.getIndex()
            return readNextString();
        }

        @Override
        public int size()
        {
            return entries.size();
        }

        private byte[] readNext()
        {
            try {
                stream.read(castBuffer.array());
                int n = castBuffer.getInt(0);
                byte[] b = new byte[n];  // here should be able to use a pooled buffer because read data is ignored if readNextString doesn't call this method
                stream.read(b);

                current++;

                return b;
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        private TargetFileInfo readNextString()
        {
            return (TargetFileInfo) SerializationUtils.deserialize(readNext());
        }
    }
}
