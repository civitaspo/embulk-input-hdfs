package org.embulk.input.hdfs;

import java.util.List;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.regex.Pattern;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Created by takahiro.nakayama on 2/20/16.
 * Ported from https://github.com/embulk/embulk-input-s3/blob/master/embulk-input-s3/src/main/java/org/embulk/input/s3/FileList.java
 * and Modified for this package.
 */
public class PartialFileList
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
        private long start;
        private long end;
        private boolean canDecompress;

        @JsonCreator
        public Entry(
                @JsonProperty("index") int index,
                @JsonProperty("start") long start,
                @JsonProperty("end") long end,
                @JsonProperty("can_decompress") boolean canDecompress)
        {
            this.index = index;
            this.start = start;
            this.end = end;
            this.canDecompress = canDecompress;
        }

        @JsonProperty("index")
        public int getIndex()
        {
            return index;
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

        @JsonProperty("can_decompress")
        public boolean getCanDecompress()
        {
            return canDecompress;
        }

        @JsonIgnore
        public long getSize()
        {
            return getEnd() - getStart();
        }
    }

    public static class Builder
    {
        private final ByteArrayOutputStream binary;
        private final OutputStream stream;
        private final List<Entry> entries = new ArrayList<>();
        private String last = null;

        private int limitCount = Integer.MAX_VALUE;
        private long minTaskSize = 1;
        private Pattern pathMatchPattern;

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
        public synchronized boolean add(String path, long start, long end, boolean canDecompress)
        {
            // TODO throw IllegalStateException if stream is already closed

            if (!needsMore()) {
                return false;
            }

            if (!pathMatchPattern.matcher(path).find()) {
                return false;
            }

            int index = entries.size();
            entries.add(new Entry(index, start, end, canDecompress));

            byte[] data = path.getBytes(StandardCharsets.UTF_8);
            castBuffer.putInt(0, data.length);
            try {
                stream.write(castBuffer.array());
                stream.write(data);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            last = path;
            return true;
        }

        public PartialFileList build()
        {
            try {
                stream.close();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            return new PartialFileList(binary.toByteArray(), getSplits(entries), Optional.fromNullable(last));
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
    private final Optional<String> last;

    @JsonCreator
    public PartialFileList(
            @JsonProperty("data") byte[] data,
            @JsonProperty("tasks") List<List<Entry>> tasks,
            @JsonProperty("last") Optional<String> last)
    {
        this.data = data;
        this.tasks = tasks;
        this.last = last;
    }

    @JsonIgnore
    public Optional<String> getLastPath(Optional<String> lastLastPath)
    {
        if (last.isPresent()) {
            return last;
        }
        return lastLastPath;
    }

    @JsonIgnore
    public int getTaskCount()
    {
        return tasks.size();
    }

    @JsonIgnore
    public List<PartialFile> get(int i)
    {
        return new EntryList(data, tasks.get(i));
    }

    @JsonProperty("data")
    public byte[] getData()
    {
        return data;
    }

    @JsonProperty("tasks")
    public List<List<Entry>> getTasks()
    {
        return tasks;
    }

    @JsonProperty("last")
    public Optional<String> getLast()
    {
        return last;
    }

    private class EntryList
            extends AbstractList<PartialFile>
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
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            this.current = 0;
        }

        @Override
        public synchronized PartialFile get(int i)
        {
            Entry entry = entries.get(i);
            if (entry.getIndex() < current) {
                // rewind to the head
                try {
                    stream.close();
                    stream = new BufferedInputStream(new GZIPInputStream(new ByteArrayInputStream(data)));
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
                current = 0;
            }

            while (current < entry.getIndex()) {
                readNext();
            }
            // now current == e.getIndex()
            return new PartialFile(readNextString(),
                    entry.getStart(), entry.getEnd(), entry.getCanDecompress());
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
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        private String readNextString()
        {
            return new String(readNext(), StandardCharsets.UTF_8);
        }
    }
}