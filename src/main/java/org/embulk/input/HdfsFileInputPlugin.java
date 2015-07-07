package org.embulk.input;

import java.util.List;
import org.embulk.config.CommitReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;

public class HdfsFileInputPlugin
        implements FileInputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("property1")
        public String getProperty1();

        @Config("property2")
        @ConfigDefault("0")
        public int getProperty2();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // taskCount is usually number of input files
        int taskCount = 1;  // number of run() method calls

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

        // TODO
        throw new UnsupportedOperationException("The 'open' method needs to be implemented");
    }
}
