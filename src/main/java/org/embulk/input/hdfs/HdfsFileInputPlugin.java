package org.embulk.input.hdfs;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathNotFoundException;
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
import org.embulk.spi.util.InputStreamTransactionalFileInput;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HdfsFileInputPlugin
        implements FileInputPlugin
{
    private static final Logger logger = Exec.getLogger(HdfsFileInputPlugin.class);
    private static FileSystem fs;

    public interface PluginTask
            extends Task
    {
        @Config("config_files")
        @ConfigDefault("[]")
        public List<String> getConfigFiles();

        @Config("config")
        @ConfigDefault("{}")
        public Map<String, String> getConfig();

        @Config("path")
        public String getPath();

        @Config("rewind_seconds")
        @ConfigDefault("0")
        public int getRewindSeconds();

        @Config("partition")
        @ConfigDefault("true")
        public boolean getPartition();

        @Config("num_partitions") // this parameter is the approximate value.
        @ConfigDefault("-1")      // Default: Runtime.getRuntime().availableProcessors()
        public long getApproximateNumPartitions();

        @Config("skip_header_lines") // Skip this number of lines first. Set 1 if the file has header line.
        @ConfigDefault("0")          // The reason why the parameter is configured is that this plugin splits files.
        public int getSkipHeaderLines();

        public List<HdfsPartialFile> getFiles();

        public void setFiles(List<HdfsPartialFile> hdfsFiles);

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // listing Files
        String pathString = strftime(task.getPath(), task.getRewindSeconds());
        try {
            List<String> originalFileList = buildFileList(getFs(task), pathString);

            if (originalFileList.isEmpty()) {
                throw new PathNotFoundException(pathString);
            }

            logger.debug("embulk-input-hdfs: Loading target files: {}", originalFileList);
            task.setFiles(allocateHdfsFilesToTasks(task, getFs(task), originalFileList));
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        // log the detail of partial files.
        for (HdfsPartialFile partialFile : task.getFiles()) {
            logger.debug("embulk-input-hdfs: target file: {}, start: {}, end: {}",
                    partialFile.getPath(), partialFile.getStart(), partialFile.getEnd());
        }

        // number of processors is same with number of targets
        int taskCount = task.getFiles().size();
        logger.info("embulk-input-hdfs: task size: {}", taskCount);

        return resume(task.dump(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            int taskCount,
            FileInputPlugin.Control control)
    {
        control.run(taskSource, taskCount);

        ConfigDiff configDiff = Exec.newConfigDiff();

        // usually, yo use last_path
        //if (task.getFiles().isEmpty()) {
        //    if (task.getLastPath().isPresent()) {
        //        configDiff.set("last_path", task.getLastPath().get());
        //    }
        //} else {
        //    List<String> files = new ArrayList<String>(task.getFiles());
        //    Collections.sort(files);
        //    configDiff.set("last_path", files.get(files.size() - 1));
        //}

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

        InputStream input;
        final HdfsPartialFile file = task.getFiles().get(taskIndex);
        try {
            if (file.getStart() > 0 && task.getSkipHeaderLines() > 0) {
                input = new SequenceInputStream(getHeadersInputStream(task, file), openInputStream(task, file));
            }
            else {
                input = openInputStream(task, file);
            }
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        return new InputStreamTransactionalFileInput(task.getBufferAllocator(), input)
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

    private InputStream getHeadersInputStream(PluginTask task, HdfsPartialFile partialFile)
            throws IOException
    {
        FileSystem fs = getFs(task);
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        int skippedHeaders = 0;

        try (BufferedInputStream in = new BufferedInputStream(fs.open(new Path(partialFile.getPath())))) {
            while (true) {
                int c = in.read();
                if (c < 0) {
                    break;
                }

                header.write(c);

                if (c == '\n') {
                    skippedHeaders++;
                }
                else if (c == '\r') {
                    int c2 = in.read();
                    if (c2 == '\n') {
                        header.write(c2);
                    }
                    skippedHeaders++;
                }

                if (skippedHeaders >= task.getSkipHeaderLines()) {
                    break;
                }
            }
        }
        header.close();
        return new ByteArrayInputStream(header.toByteArray());
    }

    private static HdfsPartialFileInputStream openInputStream(PluginTask task, HdfsPartialFile partialFile)
            throws IOException
    {
        FileSystem fs = getFs(task);
        InputStream original = fs.open(new Path(partialFile.getPath()));
        return new HdfsPartialFileInputStream(original, partialFile.getStart(), partialFile.getEnd());
    }

    private static FileSystem getFs(final PluginTask task)
        throws IOException
    {
        if (fs == null) {
            setFs(task);
            return fs;
        }
        else {
            return fs;
        }
    }

    private static FileSystem setFs(final PluginTask task)
            throws IOException
    {
        Configuration configuration = new Configuration();

        for (String configFile : task.getConfigFiles()) {
            File file = new File(configFile);
            configuration.addResource(file.toURI().toURL());
        }

        for (Map.Entry<String, String> entry : task.getConfig().entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }

        // For debug
        for (Map.Entry<String, String> entry : configuration) {
            logger.trace("{}: {}", entry.getKey(), entry.getValue());
        }
        logger.debug("Resource Files: {}", configuration);

        return FileSystem.get(configuration);
    }

    private String strftime(final String raw, final int rewindSeconds)
    {
        ScriptingContainer jruby = new ScriptingContainer();
        Object resolved = jruby.runScriptlet(
                String.format("(Time.now - %s).strftime('%s')", String.valueOf(rewindSeconds), raw));
        return resolved.toString();
    }

    private List<String> buildFileList(final FileSystem fs, final String pathString)
            throws IOException
    {
        List<String> fileList = new ArrayList<>();
        Path rootPath = new Path(pathString);

        final FileStatus[] entries = fs.globStatus(rootPath);
        // `globStatus` does not throw PathNotFoundException.
        // return null instead.
        // see: https://github.com/apache/hadoop/blob/branch-2.7.0/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/Globber.java#L286
        if (entries == null) {
            return fileList;
        }

        for (FileStatus entry : entries) {
            if (entry.isDirectory()) {
                fileList.addAll(lsr(fs, entry));
            }
            else {
                fileList.add(entry.getPath().toString());
            }
        }

        return fileList;
    }

    private List<String> lsr(final FileSystem fs, FileStatus status)
            throws IOException
    {
        List<String> fileList = new ArrayList<>();
        if (status.isDirectory()) {
            for (FileStatus entry : fs.listStatus(status.getPath())) {
                fileList.addAll(lsr(fs, entry));
            }
        }
        else {
            fileList.add(status.getPath().toString());
        }
        return fileList;
    }

    private List<HdfsPartialFile> allocateHdfsFilesToTasks(final PluginTask task, final FileSystem fs, final List<String> fileList)
            throws IOException
    {
        List<Path> pathList = Lists.transform(fileList, new Function<String, Path>()
        {
            @Nullable
            @Override
            public Path apply(@Nullable String input)
            {
                return new Path(input);
            }
        });

        long totalFileLength = 0;
        for (Path path : pathList) {
            totalFileLength += fs.getFileStatus(path).getLen();
        }

        // TODO: optimum allocation of resources
        long approximateNumPartitions =
                (task.getApproximateNumPartitions() <= 0) ? Runtime.getRuntime().availableProcessors() : task.getApproximateNumPartitions();
        long partitionSizeByOneTask = totalFileLength / approximateNumPartitions;
        if (partitionSizeByOneTask <= 0) {
            partitionSizeByOneTask = 1;
        }

        List<HdfsPartialFile> hdfsPartialFiles = new ArrayList<>();
        for (Path path : pathList) {
            long fileLength = fs.getFileStatus(path).getLen(); // declare `fileLength` here because this is used below.
            if (fileLength <= 0) {
                logger.info("embulk-input-hdfs: Skip the 0 byte target file: {}", path);
                continue;
            }

            long numPartitions;
            if (path.toString().endsWith(".gz") || path.toString().endsWith(".bz2") || path.toString().endsWith(".lzo")) {
                numPartitions = 1;
            }
            else if (!task.getPartition()) {
                numPartitions = 1;
            }
            else {
                numPartitions = ((fileLength - 1) / partitionSizeByOneTask) + 1;
            }

            HdfsFilePartitioner partitioner = new HdfsFilePartitioner(fs, path, numPartitions);
            hdfsPartialFiles.addAll(partitioner.getHdfsPartialFiles());
        }

        return hdfsPartialFiles;
    }
}
