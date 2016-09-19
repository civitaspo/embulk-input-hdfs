package org.embulk.input.hdfs;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.jruby.embed.ScriptingContainer;

public class Strftime
{
    interface Task
    {
        @Config("rewind_seconds")
        @ConfigDefault("0")
        int getRewindSeconds();

        @ConfigInject
        ScriptingContainer getJRuby();
    }

    private final int rewindSeconds;
    private final ScriptingContainer jruby;

    public Strftime(Task task)
    {
        this.rewindSeconds = task.getRewindSeconds();
        this.jruby = task.getJRuby();
    }

    public String format(String format)
    {
        String script = String.format("(Time.now - %d).strftime('%s')", rewindSeconds, format);
        return jruby.runScriptlet(script).toString();
    }
}
