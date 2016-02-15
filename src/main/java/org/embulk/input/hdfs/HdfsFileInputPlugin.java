package org.embulk.input.hdfs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.hdfs.file.HDFSFile;
import org.embulk.input.hdfs.file.HDFSPartialFile;
import org.embulk.input.hdfs.util.ConfigurationFactory;
import org.embulk.input.hdfs.util.HDFSFileFactory;
import org.embulk.input.hdfs.util.HDFSFilePartitioner;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamTransactionalFileInput;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HdfsFileInputPlugin
        implements FileInputPlugin
{
    public interface PluginTask
            extends Task
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

        @ConfigInject
        ScriptingContainer getJRuby();
    }

    private static final Logger logger = Exec.getLogger(HdfsFileInputPlugin.class);
    private Optional<Configuration> configurationContainer = Optional.absent();
    private Optional<FileSystem> fsContainer = Optional.absent();
    private Optional<List<HDFSPartialFile>> hdfsPartialFilesContainer = Optional.absent();

    private void setContainers(PluginTask task)
    {
        if (!configurationContainer.isPresent()) {
            setConfiguration(ConfigurationFactory.fromTask(task));
        }

        if (!fsContainer.isPresent()) {
            try {
                setFileSystem(FileSystem.get(getConfiguration()));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void setConfiguration(Configuration configuration)
    {
        configurationContainer = Optional.of(configuration);
    }

    private Configuration getConfiguration()
    {
        return configurationContainer.get();
    }

    private void setFileSystem(FileSystem fs)
    {
        fsContainer = Optional.of(fs);
    }

    private FileSystem getFS()
    {
        return fsContainer.get();
    }

    @VisibleForTesting
    public void setHDFSPartialFiles(List<HDFSPartialFile> hdfsPartialFiles)
    {
        hdfsPartialFilesContainer = Optional.of(hdfsPartialFiles);
    }

    @VisibleForTesting
    public List<HDFSPartialFile> getHDFSPartialFiles()
    {
        return hdfsPartialFilesContainer.get();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        setContainers(task);

        // listing Files
        try {
            String pathString = strftime(task, task.getPath(), task.getRewindSeconds());
            Path rootPath = new Path(pathString);

            List<Path> originalFileList = buildFileList(getFS(), rootPath);

            if (originalFileList.isEmpty()) {
                throw new PathNotFoundException(pathString);
            }

            logger.debug("embulk-input-hdfs: Loading target files: {}", originalFileList);
            setHDFSPartialFiles(allocateHdfsFilesToNumTasks(task, originalFileList));
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        // log the detail of partial files.
        for (HDFSPartialFile partialFile : getHDFSPartialFiles()) {
            logger.debug("embulk-input-hdfs: target file: {}, start: {}, end: {}",
                    partialFile.getPath(), partialFile.getStart(), partialFile.getEnd());
        }

        // number of processors is same with number of targets
        int taskCount = getHDFSPartialFiles().size();
        logger.info("embulk-input-hdfs: task size: {}", taskCount);

        return resume(task.dump(), taskCount, control);
    }

    @VisibleForTesting
    String strftime(final PluginTask task, final String format, final int rewindSeconds)
    {
        String script = String.format("(Time.now - %d).strftime('%s')", rewindSeconds, format);
        return task.getJRuby().runScriptlet(script).toString();
    }

    private List<Path> buildFileList(FileSystem fs, Path rootPath)
            throws IOException
    {
        List<Path> fileList = Lists.newArrayList();

        final FileStatus[] entries = fs.globStatus(rootPath);
        // `globStatus` does not throw PathNotFoundException.
        // return null instead.
        // see: https://github.com/apache/hadoop/blob/branch-2.7.0/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/Globber.java#L286
        if (entries == null) {
            return fileList;
        }

        for (FileStatus entry : entries) {
            if (entry.isDirectory()) {
                fileList.addAll(listRecursive(fs, entry));
            }
            else {
                fileList.add(entry.getPath());
            }
        }

        return fileList;
    }

    private List<Path> listRecursive(final FileSystem fs, FileStatus status)
            throws IOException
    {
        List<Path> fileList = new ArrayList<>();
        if (status.isDirectory()) {
            for (FileStatus entry : fs.listStatus(status.getPath())) {
                fileList.addAll(listRecursive(fs, entry));
            }
        }
        else {
            fileList.add(status.getPath());
        }
        return fileList;
    }

    private List<HDFSPartialFile> allocateHdfsFilesToNumTasks(PluginTask task, List<Path> pathList)
            throws IOException
    {
        HDFSFileFactory factory = new HDFSFileFactory(getConfiguration());
        FileSystem fs = getFS();

        Map<HDFSFile, Long> hdfsFileLengthMap = Maps.newHashMap();
        long totalFileLength = 0;
        for (Path path : pathList) {
            HDFSFile file = factory.create(path, task.getDecompression());
            long fileLength = file.getLength(fs);

            if (fileLength <= 0) {
                logger.info("`embulk-input-hdfs`: Skip the 0 byte target file: {}", path);
                continue;
            }

            hdfsFileLengthMap.put(file, fileLength);
            totalFileLength += fileLength;
        }
        if (totalFileLength <= 0) {
            throw new PathIOException(task.getPath(), "`embulk-input-hdfs`: All files are empty");
        }

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

        List<HDFSPartialFile> hdfsPartialFiles = Lists.newArrayList();
        for (Map.Entry<HDFSFile, Long> entry : hdfsFileLengthMap.entrySet()) {
            HDFSFile file = entry.getKey();
            long fileLength = entry.getValue();

            long numPartitions;
            if (task.getPartition()) {
                if (file.canDecompress()) {
                    numPartitions = ((fileLength - 1) / partitionSizeByOneTask) + 1;
                }
                else if (file.getCodec() != null) { // if not null, the file is compressed.
                    numPartitions = 1;
                }
                else {
                    numPartitions = ((fileLength - 1) / partitionSizeByOneTask) + 1;
                }
            }
            else {
                numPartitions = 1;
            }

            HDFSFilePartitioner partitioner = new HDFSFilePartitioner(file, fileLength, numPartitions);
            hdfsPartialFiles.addAll(partitioner.generateHDFSPartialFiles());
        }

        return hdfsPartialFiles;
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
        setContainers(task);

        InputStream input;
        final HDFSPartialFile file = getHDFSPartialFiles().get(taskIndex);
        try {
            if (file.getStart() > 0 && task.getSkipHeaderLines() > 0) {
                input = file.createInputStreamWithHeaders(getFS(), task.getSkipHeaderLines());
            }
            else {
                input = file.createInputStream(getFS());
            }
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        return new InputStreamTransactionalFileInput(Exec.getBufferAllocator(), input)
        {
            @Override
            public void abort()
            { }

            @Override
            public TaskReport commit()
            {
                return Exec.newTaskReport();
            }
        };
    }
}
