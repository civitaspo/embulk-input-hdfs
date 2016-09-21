package org.embulk.input.hdfs;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathNotFoundException;
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
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

public class HdfsFileInputPlugin
        implements FileInputPlugin
{
    public interface PluginTask
            extends Task, TargetFileInfoList.Task, ConfigurationFactory.Task, Strftime.Task
    {
        @Config("path")
        String getPath();

        @Config("partition")
        @ConfigDefault("true")
        boolean getWillPartition();

        @Config("num_partitions") // this parameter is the approximate value.
        @ConfigDefault("-1")      // Default: Runtime.getRuntime().availableProcessors()
        long getApproximateNumPartitions();

        @Config("skip_header_lines") // Skip this number of lines first. Set 1 if the file has header line.
        @ConfigDefault("0")          // The reason why the parameter is configured is that this plugin splits files.
        int getSkipHeaderLines();

        @Config("decompression") // if true, decompress files by using compression codec
        @ConfigDefault("false")  // when getting FileInputStream.
        boolean getWillDecompress();

        TargetFileInfoList getTargetFileInfoList();
        void setTargetFileInfoList(TargetFileInfoList targetFileInfoList);
    }

    private static final Logger logger = Exec.getLogger(HdfsFileInputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        if (task.getWillPartition() && task.getWillDecompress()) {
            logger.info("embulk-input-hdfs: Please be sure that the target files cannot be partitioned if they are compressed.");
        }

        Configuration conf = ConfigurationFactory.create(task);

        // listing Files
        try {
            FileSystem fs = FileSystem.get(conf);

            String pathString = new Strftime(task).format(task.getPath());
            Path rootPath = new Path(pathString);

            List<FileStatus> statusList = listFileStatuses(fs, rootPath);

            if (statusList.isEmpty()) {
                throw new PathNotFoundException(pathString);
            }

            for (FileStatus status : statusList) {
                logger.debug("embulk-input-hdfs: Loading paths: {}, length: {}", status.getPath(), status.getLen());
            }

            TargetFileInfoList list = buildTargetFileInfoList(task, statusList);
            task.setTargetFileInfoList(list);
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw Throwables.propagate(e);
        }

        // number of processors is same with number of targets
        int taskCount = task.getTargetFileInfoList().getTaskCount();
        logger.info("embulk-input-hdfs: task size: {}", taskCount);

        return resume(task.dump(), taskCount, control);
    }

    private List<FileStatus> listFileStatuses(FileSystem fs, Path rootPath)
            throws IOException
    {
        List<FileStatus> statusList = Lists.newArrayList();

        FileStatus[] entries = fs.globStatus(rootPath);
        // `globStatus` does not throw PathNotFoundException.
        // return null instead.
        // see: https://github.com/apache/hadoop/blob/branch-2.7.0/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/Globber.java#L286
        if (entries == null) {
            return statusList;
        }

        for (FileStatus entry : entries) {
            if (entry.isDirectory()) {
                // TODO: use fs.listFiles(entry.getPath(), true); ?
                List<FileStatus> subEntries = listRecursive(fs, entry);
                statusList.addAll(subEntries);
            }
            else {
                statusList.add(entry);
            }
        }

        return statusList;
    }

    private List<FileStatus> listRecursive(FileSystem fs, FileStatus status)
            throws IOException
    {
        List<FileStatus> statusList = Lists.newArrayList();
        if (status.isDirectory()) {
            FileStatus[] entries = fs.listStatus(status.getPath());
            for (FileStatus entry : entries) {
                statusList.addAll(listRecursive(fs, entry));
            }
        }
        else {
            statusList.add(status);
        }
        return statusList;
    }

    private TargetFileInfoList buildTargetFileInfoList(PluginTask task, List<FileStatus> statusList)
            throws IOException, DataException
    {
        long totalFileLength = calcTotalFilesLength(statusList);
        if (totalFileLength <= 0) {
            logger.warn("embulk-input-hdfs: All files are empty: {}", task.getPath());
            return TargetFileInfoList.builder(task).build();
        }

        long partitionSizeByOneTask = calcApproximatePartitionSizeByOneTask(task, totalFileLength);

        Configuration conf = ConfigurationFactory.create(task);
        TargetFileInfoList.Builder builder = TargetFileInfoList.builder(task);
        for (FileStatus status : statusList) {
            if (status.getLen() <= 0) {
                logger.info("embulk-input-hdfs: Skip the 0 byte target file: {}", status.getPath());
                continue;
            }

            long numPartitions = 1; // default is no partition.
            if (isPartitionable(task, conf, status)) {
                numPartitions = ((status.getLen() - 1) / partitionSizeByOneTask) + 1;
            }

            for (long i = 0; i < numPartitions; i++) {
                long start = status.getLen() * i / numPartitions;
                long end = status.getLen() * (i + 1) / numPartitions;
                if (start < end) {
                    TargetFileInfo targetFileInfo = new TargetFileInfo.Builder()
                            .pathString(status.getPath().toString())
                            .start(start)
                            .end(end)
                            .isDecompressible(isDecompressible(task, conf, status))
                            .isPartitionable(isPartitionable(task, conf, status))
                            .numHeaderLines(task.getSkipHeaderLines())
                            .build();
                    builder.add(targetFileInfo);
                }
            }
        }
        return builder.build();
    }

    private boolean isDecompressible(PluginTask task, Configuration conf, FileStatus status)
    {
        return task.getWillDecompress() && new CompressionCodecFactory(conf).getCodec(status.getPath()) != null;
    }

    private boolean isPartitionable(PluginTask task, Configuration conf, FileStatus status)
    {
        return task.getWillPartition() && !isDecompressible(task, conf, status);
    }

    private long calcTotalFilesLength(List<FileStatus> statusList)
            throws IOException
    {
        long total = 0L;
        for (FileStatus status : statusList) {
            total += status.getLen();
        }
        return total;
    }

    private long calcApproximatePartitionSizeByOneTask(PluginTask task, long totalFilesLength)
    {
        long numPartitions = task.getApproximateNumPartitions();
        if (numPartitions <= 0) {
            numPartitions = Runtime.getRuntime().availableProcessors();
        }
        // TODO: optimum allocation of resources
        long partitionSizeByOneTask = totalFilesLength / numPartitions;
        if (partitionSizeByOneTask <= 0) {
            partitionSizeByOneTask = 1;
        }
        return partitionSizeByOneTask;
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
        try {
            return new HdfsFileInput(task, taskIndex);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public class HdfsFileInput
            extends InputStreamFileInput
            implements TransactionalFileInput
    {
        public HdfsFileInput(PluginTask task, int taskIndex)
                throws IOException
        {
            super(Exec.getBufferAllocator(), new SingleFileProvider(task, taskIndex));
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
        private final TargetFileInputStreamFactory factory;
        private final Iterator<TargetFileInfo> iterator;

        public SingleFileProvider(PluginTask task, int taskIndex)
                throws IOException
        {
            this.factory = new TargetFileInputStreamFactory(FileSystem.get(ConfigurationFactory.create(task)));
            this.iterator = task.getTargetFileInfoList().get(taskIndex).iterator();
        }

        @Override
        public InputStream openNext() throws IOException
        {
            if (!iterator.hasNext()) {
                return null;
            }
            return factory.create(iterator.next());
        }

        @Override
        public void close()
        {
        }
    }
}
