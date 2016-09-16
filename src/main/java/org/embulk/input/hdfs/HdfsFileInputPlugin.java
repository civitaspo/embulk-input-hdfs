package org.embulk.input.hdfs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HdfsFileInputPlugin
        implements FileInputPlugin
{
    public interface PluginTask
            extends Task, PartialFileList.Task
    {
        @Config("config_files")
        @ConfigDefault("[]")
        List<String> getConfigFiles();

        @Config("config")
        @ConfigDefault("{}")
        Map<String, String> getConfig();

        @Config("path")
        String getPath();

        @Config("rewind_seconds")
        @ConfigDefault("0")
        int getRewindSeconds();

        @Config("partition")
        @ConfigDefault("true")
        boolean getPartition();

        @Config("num_partitions") // this parameter is the approximate value.
        @ConfigDefault("-1")      // Default: Runtime.getRuntime().availableProcessors()
        long getApproximateNumPartitions();

        @Config("skip_header_lines") // Skip this number of lines first. Set 1 if the file has header line.
        @ConfigDefault("0")          // The reason why the parameter is configured is that this plugin splits files.
        int getSkipHeaderLines();

        @Config("decompression") // if true, decompress files by using compression codec
        @ConfigDefault("false")  // when getting FileInputStream.
        boolean getDecompression();

        PartialFileList getPartialFileList();
        void setPartialFileList(PartialFileList partialFileList);

        @ConfigInject
        ScriptingContainer getJRuby();

        @ConfigInject
        BufferAllocator getBufferAllocator();
    }

    private static final Logger logger = Exec.getLogger(HdfsFileInputPlugin.class);
    private Optional<Configuration> configurationContainer = Optional.absent();

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        if (task.getPartition() && task.getDecompression()) {
            logger.info("Please be sure that the target files cannot be partitioned if they are compressed.");
        }

        Configuration configuration = getConfiguration(task);

        // listing Files
        try {
            FileSystem fs = getFS(configuration);

            String pathString = strftime(task.getJRuby(), task.getPath(), task.getRewindSeconds());
            Path rootPath = new Path(pathString);

            List<Path> originalFileList = buildOriginalFileList(fs, rootPath);

            if (originalFileList.isEmpty()) {
                throw new PathNotFoundException(pathString);
            }

            logger.debug("embulk-input-hdfs: Loading target files: {}", originalFileList);
            PartialFileList list = buildPartialFileList(task, originalFileList);
            task.setPartialFileList(list);
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        // number of processors is same with number of targets
        int taskCount = task.getPartialFileList().getTaskCount();
        logger.info("embulk-input-hdfs: task size: {}", taskCount);

        return resume(task.dump(), taskCount, control);
    }

    private Configuration getConfiguration(PluginTask task)
    {
        if (configurationContainer.isPresent()) {
            return configurationContainer.get();
        }

        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.addConfigFiles(task.getConfigFiles());
        builder.addConfigMap(task.getConfig());
        configurationContainer = Optional.of(builder.build());
        return configurationContainer.get();
    }

    private FileSystem getFS(Configuration configuration)
    {
        try {
            return FileSystem.get(configuration);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @VisibleForTesting
    String strftime(final ScriptingContainer jruby, final String format, final int rewindSeconds)
    {
        String script = String.format("(Time.now - %d).strftime('%s')", rewindSeconds, format);
        return jruby.runScriptlet(script).toString();
    }

    private List<Path> buildOriginalFileList(FileSystem fs, Path rootPath)
    {
        List<Path> fileList = Lists.newArrayList();

        final FileStatus[] entries;
        try {
            entries = fs.globStatus(rootPath);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        // `globStatus` does not throw PathNotFoundException.
        // return null instead.
        // see: https://github.com/apache/hadoop/blob/branch-2.7.0/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/Globber.java#L286
        if (entries == null) {
            return fileList;
        }

        for (FileStatus entry : entries) {
            if (entry.isDirectory()) {
                List<Path> subEntries = listRecursive(fs, entry);
                fileList.addAll(subEntries);
            }
            else {
                fileList.add(entry.getPath());
            }
        }

        return fileList;
    }

    private List<Path> listRecursive(final FileSystem fs, FileStatus status)
    {
        List<Path> fileList = Lists.newArrayList();
        if (status.isDirectory()) {
            FileStatus[] entries;
            try {
                entries = fs.listStatus(status.getPath());
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            for (FileStatus entry : entries) {
                fileList.addAll(listRecursive(fs, entry));
            }
        }
        else {
            fileList.add(status.getPath());
        }
        return fileList;
    }

    private PartialFileList buildPartialFileList(PluginTask task, List<Path> pathList)
    {
        Configuration configuration = getConfiguration(task);
        FileSystem fs = getFS(configuration);
        boolean shouldPartition = task.getPartition();
        boolean shouldDecompress = task.getDecompression();

        Map<Path, Long> pathLengthMap = Maps.newHashMap();
        long totalFileLength = 0;
        for (Path path : pathList) {
            long fileLength = getHdfsFileLength(fs, path, shouldDecompress);

            if (fileLength <= 0) {
                logger.info("Skip the 0 byte target file: {}", path);
                continue;
            }

            pathLengthMap.put(path, fileLength);
            totalFileLength += fileLength;
        }
        if (totalFileLength <= 0) {
            throw Throwables.propagate(new PathIOException(task.getPath(), "All files are empty"));
        }

        PartialFileList.Builder builder = new PartialFileList.Builder(task);

        // TODO: optimum allocation of resources
        final long approximateNumPartitions;
        if (task.getApproximateNumPartitions() <= 0) {
            approximateNumPartitions = Runtime.getRuntime().availableProcessors();
        }
        else {
            approximateNumPartitions = task.getApproximateNumPartitions();
        }

        long partitionSizeByOneTask = totalFileLength / approximateNumPartitions;
        if (partitionSizeByOneTask <= 0) {
            partitionSizeByOneTask = 1;
        }

        for (Map.Entry<Path, Long> entry : pathLengthMap.entrySet()) {
            Path path = entry.getKey();
            long fileLength = entry.getValue();

            long numPartitions;
            if (shouldPartition) {
                if (shouldDecompress && getHdfsFileCompressionCodec(fs, path) != null) {
                    numPartitions = ((fileLength - 1) / partitionSizeByOneTask) + 1;
                }
                else if (getHdfsFileCompressionCodec(fs, path) != null) { // if not null, the file is compressed.
                    numPartitions = 1;
                }
                else {
                    numPartitions = ((fileLength - 1) / partitionSizeByOneTask) + 1;
                }
            }
            else {
                numPartitions = 1;
            }

            for (long i = 0; i < numPartitions; i++) {
                long start = fileLength * i / numPartitions;
                long end = fileLength * (i + 1) / numPartitions;
                if (start < end) {
                    logger.debug("PartialFile: path {}, start: {}, end: {}", path, start, end);
                    builder.add(path.toString(), start, end, shouldDecompress && getHdfsFileCompressionCodec(fs, path) != null);
                }
            }
        }

        return builder.build();
    }

    private Long getHdfsFileLength(FileSystem fs, Path path, boolean shouldDecompression)
    {
        CompressionCodec codec = getHdfsFileCompressionCodec(fs, path);
        if (codec == null) {
            try {
                return fs.getFileStatus(path).getLen();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
        else if (!shouldDecompression) {
            try {
                return fs.getFileStatus(path).getLen();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
        else {
            long fileLength = 0;
            try (InputStream is = codec.createInputStream(fs.open(path))) {
                while (is.read() > 0) {
                    fileLength++;
                }
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            return fileLength;
        }
    }

    private CompressionCodec getHdfsFileCompressionCodec(FileSystem fs, Path path)
    {
        return getHdfsFileCompressionCodec(fs.getConf(), path);
    }

    private CompressionCodec getHdfsFileCompressionCodec(Configuration configuration, Path path)
    {
        return new CompressionCodecFactory(configuration).getCodec(path);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            int taskCount,
            FileInputPlugin.Control control)
    {
        control.run(taskSource, taskCount);
        ConfigDiff configDiff = Exec.newConfigDiff();
        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
            int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        return new HdfsFileInput(task, taskIndex);
    }

    public class HdfsFileInput
            extends InputStreamFileInput
            implements TransactionalFileInput
    {

        public HdfsFileInput(PluginTask task, int taskIndex)
        {
            super(task.getBufferAllocator(), new SingleFileProvider(task, taskIndex));
        }

        @Override
        public void abort()
        {
        }

        @Override
        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }
    }

    // TODO create single-file InputStreamFileInput utility
    private class SingleFileProvider
            implements InputStreamFileInput.Provider
    {
        private final FileSystem fs;
        private final int numHeaderLines;
        private final Iterator<PartialFile> iterator;

        public SingleFileProvider(PluginTask task, int taskIndex)
        {
            this.fs = getFS(getConfiguration(task));
            this.numHeaderLines = task.getSkipHeaderLines();
            this.iterator = task.getPartialFileList().get(taskIndex).iterator();
        }

        @Override
        public InputStream openNext() throws IOException
        {
            if (!iterator.hasNext()) {
                return null;
            }
            PartialFileInputStreamBuilder builder = new PartialFileInputStreamBuilder(fs, iterator.next()).withHeaders(numHeaderLines);
            return builder.build();
        }

        @Override
        public void close()
        {
        }
    }
}
