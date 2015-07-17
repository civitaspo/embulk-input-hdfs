package org.embulk.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.embulk.config.*;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

        public List<String> getTargetFiles();
        public void setTargetFiles(List<String> targetFiles);

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // prepare
        Configuration configuration = getHdfsConfiguration(task);
        FileSystem fs = getFs(configuration);
        Path inputPath = new Path(strftime(task.getInputPath(), task.getRewindSeconds()));

        // listing
        List<String> targetFiles;
        try {
            targetFiles = globRecursive(fs, inputPath);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
        logger.info("Loading target files: {}", targetFiles);
        task.setTargetFiles(targetFiles);

        // number of processors is same with number of targets
        int taskCount = targetFiles.size();

        return resume(task.dump(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileInputPlugin.Control control)
    {
        control.run(taskSource, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        int taskCount,
                        List<CommitReport> successCommitReports)
    {
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        // prepare
        Configuration configuration = getHdfsConfiguration(task);
        FileSystem fs = getFs(configuration);

        return new HdfsFileInput(task, fs, taskIndex);
    }

    private Configuration getHdfsConfiguration(final PluginTask task)
    {
        Configuration configuration = new Configuration();

        for (Object configFile : task.getConfigFiles()) {
            configuration.addResource(configFile.toString());
        }
        configuration.reloadConfiguration();

        for (Map.Entry<String, String> entry: task.getConfig().entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }

        return configuration;
    }

    private FileSystem getFs(final Configuration configuration)
    {
        try {
            FileSystem fs = FileSystem.get(configuration);
            return fs;
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private String strftime(final String raw, final int rewind_seconds)
    {
        ScriptingContainer jruby = new ScriptingContainer();
        Object resolved = jruby.runScriptlet(
                String.format("(Time.now - %s).strftime('%s')", String.valueOf(rewind_seconds), raw));
        return resolved.toString();
    }

    private List<String> globRecursive(final FileSystem fs, final Path hdfsPath) throws IOException
    {
        List<String> container = new ArrayList<String>();
        for (FileStatus entry : fs.globStatus(hdfsPath)) {
            if (entry.isDirectory()) {
                container.addAll(listRecursive(fs, entry));
            }
            else {
                container.add(entry.getPath().toString());
            }
        }
        return container;
    }

    private List<String> listRecursive(final FileSystem fs, FileStatus status) throws IOException {
        List<String> container = new ArrayList<String>();
        if (status.isDirectory()) {
            for (FileStatus entry : fs.listStatus(status.getPath())) {
                container.addAll(listRecursive(fs, entry));
            }
        }
        else {
            container.add(status.getPath().toString());
        }
        return container;
    }



//    private List<String> listUniquify(List<String> stringList)
//    {
//        Set<String> set = new HashSet<String>();
//        set.addAll(stringList);
//        List<String> uniqueStringList = new ArrayList<String>();
//        uniqueStringList.addAll(set);
//        return uniqueStringList;
//    }

    public static class HdfsFileInput extends InputStreamFileInput implements TransactionalFileInput
    {
        private static class HdfsFileProvider implements InputStreamFileInput.Provider
        {
            private final FileSystem fs;
            private final Path hdfsPath;
            private boolean opened = false;

            public HdfsFileProvider(PluginTask task, FileSystem fs, int taskIndex)
            {
                this.fs = fs;
                this.hdfsPath = new Path(task.getTargetFiles().get(taskIndex));
            }

            @Override
            public InputStream openNext() throws IOException
            {
                if (opened) {
                    return null;
                }

                opened = true;
                return fs.open(hdfsPath);
            }

            @Override
            public void close()
            {
            }
        }

        public HdfsFileInput(PluginTask task, FileSystem fs, int taskIndex)
        {
            super(task.getBufferAllocator(), new HdfsFileProvider(task, fs, taskIndex));
        }

        @Override
        public void close()
        {
        }

        @Override
        public void abort()
        {
        }

        @Override
        public CommitReport commit()
        {
            return Exec.newCommitReport();
        }
    }
}
