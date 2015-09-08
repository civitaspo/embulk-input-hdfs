package org.embulk.input.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.embulk.config.TaskReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.InputStreamTransactionalFileInput;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

import javax.annotation.Nullable;

public class HdfsFileInputPlugin implements FileInputPlugin
{
    private static final Logger logger = Exec.getLogger(HdfsFileInputPlugin.class);

    public interface PluginTask extends Task
    {
        @Config("config_files")
        @ConfigDefault("[]")
        public List<String> getConfigFiles();

        @Config("config")
        @ConfigDefault("{}")
        public Map<String, String> getConfig();

        @Config("input_path")
        public String getInputPath();

        @Config("rewind_seconds")
        @ConfigDefault("0")
        public int getRewindSeconds();

        @Config("partition")
        @ConfigDefault("true")
        public boolean getPartition();

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
        String pathString = strftime(task.getInputPath(), task.getRewindSeconds());
        try {
            List<String> originalFileList = buildFileList(getFs(task), pathString);
            task.setFiles(allocateHdfsFilesToTasks(task, getFs(task), originalFileList));
            logger.info("Loading target files: {}", originalFileList);
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        // log the detail of partial files.
        for (HdfsPartialFile partialFile : task.getFiles()) {
            logger.info("target file: {}, start: {}, end: {}",
                    partialFile.getPath(), partialFile.getStart(), partialFile.getEnd());
        }

        // number of processors is same with number of targets
        int taskCount = task.getFiles().size();
        logger.info("task size: {}", taskCount);

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
        try {
            input = openInputStream(task, task.getFiles().get(taskIndex));
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        return new InputStreamTransactionalFileInput(task.getBufferAllocator(), input) {
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

    private static HdfsPartialFileInputStream openInputStream(PluginTask task, HdfsPartialFile partialFile) throws IOException
    {
        FileSystem fs = getFs(task);
        InputStream original = fs.open(new Path(partialFile.getPath()));
        return new HdfsPartialFileInputStream(original, partialFile.getStart(), partialFile.getEnd());
    }

    private static FileSystem getFs(final PluginTask task) throws IOException {
        Configuration configuration = new Configuration();

        for (Object configFile : task.getConfigFiles()) {
            configuration.addResource(configFile.toString());
        }
        configuration.reloadConfiguration();

        for (Map.Entry<String, String> entry: task.getConfig().entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }

        return FileSystem.get(configuration);
    }

    private String strftime(final String raw, final int rewind_seconds)
    {
        ScriptingContainer jruby = new ScriptingContainer();
        Object resolved = jruby.runScriptlet(
                String.format("(Time.now - %s).strftime('%s')", String.valueOf(rewind_seconds), raw));
        return resolved.toString();
    }

    private List<String> buildFileList(final FileSystem fs, final String pathString) throws IOException
    {
        List<String> fileList = new ArrayList<>();
        for (FileStatus entry : fs.globStatus(new Path(pathString))) {
            if (entry.isDirectory()) {
                fileList.addAll(lsr(fs, entry));
            } else {
                fileList.add(entry.getPath().toString());
            }
        }
        return fileList;
    }

    private List<String> lsr(final FileSystem fs, FileStatus status) throws IOException
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

        int totalFileLength = 0;
        for (Path path : pathList) {
            totalFileLength += fs.getFileStatus(path).getLen();
        }

        // TODO: optimum allocation of resources
        int partitionCountParameter = 3;
        int partitionSizeByOneTask = totalFileLength / (Runtime.getRuntime().availableProcessors() * partitionCountParameter);

        List<HdfsPartialFile> hdfsPartialFiles = new ArrayList<>();
        for (Path path : pathList) {
            int partitionCount;

            if (path.toString().endsWith(".gz") || path.toString().endsWith(".bz2") || path.toString().endsWith(".lzo")) {
                partitionCount = 1;
            }
            else if (!task.getPartition()) {
                partitionCount = 1;
            }
            else {
                int fileLength = (int) fs.getFileStatus(path).getLen();
                partitionCount = fileLength / partitionSizeByOneTask;
                int remainder = fileLength % partitionSizeByOneTask;

                if (remainder > 0) {
                    partitionCount++;
                }
            }

            HdfsFilePartitioner partitioner = new HdfsFilePartitioner(fs, path, partitionCount);
            hdfsPartialFiles.addAll(partitioner.getHdfsPartialFiles());
        }

        return hdfsPartialFiles;
    }
}
