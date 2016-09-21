package org.embulk.input.hdfs;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.hdfs.HdfsFileInputPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileInputRunner;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.util.Pages;
import org.embulk.standards.CsvParserPlugin;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestHdfsFileInputPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private HdfsFileInputPlugin plugin;
    private FileInputRunner runner;
    private MockPageOutput output;
    private Path path;

    @Before
    public void createResources()
    {
        plugin = new HdfsFileInputPlugin();
        runner = new FileInputRunner(runtime.getInstance(HdfsFileInputPlugin.class));
        output = new MockPageOutput();
        path = new Path(new File(getClass().getResource("/sample_01.csv").getPath()).getParent());
    }

    @Test
    public void testDefaultValues()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("path", path.toString());
        PluginTask task = config.loadConfig(PluginTask.class);
        assertEquals(path.toString(), task.getPath());
        assertEquals(Lists.newArrayList(), task.getConfigFiles());
        assertEquals(Maps.newHashMap(), task.getConfig());
        assertEquals(true, task.getWillPartition());
        assertEquals(0, task.getRewindSeconds());
        assertEquals(-1, task.getApproximateNumPartitions());
        assertEquals(0, task.getSkipHeaderLines());
        assertEquals(false, task.getWillDecompress());
    }

    @Test(expected = ConfigException.class)
    public void testRequiredValues()
    {
        ConfigSource config = Exec.newConfigSource();
        PluginTask task = config.loadConfig(PluginTask.class);
    }

    @Test
    public void testFileList()
    {
        ConfigSource config = getConfigWithDefaultValues();
        config.set("num_partitions", 1);
        plugin.transaction(config, new FileInputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource, int taskCount)
            {
                PluginTask task = taskSource.loadTask(PluginTask.class);
                List<String> fileList = Lists.transform(Lists.newArrayList(new File(path.toString()).list()), new Function<String, String>()
                {
                    @Nullable
                    @Override
                    public String apply(@Nullable String input)
                    {
                        return new File(path.toString() + "/" + input).toURI().toString();
                    }
                });

                List<String> resultFList = Lists.newArrayList();
                for (int i = 0; i < task.getTargetFileInfoList().getTaskCount();i++) {
                    for (TargetFileInfo targetFileInfo : task.getTargetFileInfoList().get(i)) {
                        resultFList.add(targetFileInfo.getPathString());
                    }
                }
                assertEquals(fileList.size(), resultFList.size());
                assert fileList.containsAll(resultFList);
                return emptyTaskReports(taskCount);
            }
        });
    }

    @Test
    public void testHdfsFileInputByOpen()
    {
        ConfigSource config = getConfigWithDefaultValues();
        config.set("num_partitions", 10);
        config.set("decompression", true);
        runner.transaction(config, new Control());
        assertRecords(config, output, 12);
    }

    @Test
    public void testHdfsFileInputByOpenWithoutPartition()
    {
        ConfigSource config = getConfigWithDefaultValues();
        config.set("partition", false);
        config.set("decompression", true);
        runner.transaction(config, new Control());
        assertRecords(config, output, 12);
    }

    @Test
    public void testHdfsFileInputByOpenWithoutCompressionCodec()
    {
        ConfigSource config = getConfigWithDefaultValues();
        config.set("partition", false);
        config.set("path", getClass().getResource("/sample_01.csv").getPath());
        runner.transaction(config, new Control());
        assertRecords(config, output, 4);
    }

    @Test
    public void testStrftime()
    {
        ConfigSource config = getConfigWithDefaultValues();
        config.set("path", "/tmp/%Y-%m-%d");
        config.set("rewind_seconds", 86400);
        PluginTask task = config.loadConfig(PluginTask.class);
        String result = new Strftime(task).format(task.getPath());
        String expected = task.getJRuby().runScriptlet("(Time.now - 86400).strftime('/tmp/%Y-%m-%d')").toString();
        assertEquals(expected, result);
    }

    private class Control
            implements InputPlugin.Control
    {
        @Override
        public List<TaskReport> run(TaskSource taskSource, Schema schema, int taskCount)
        {
            List<TaskReport> reports = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                reports.add(runner.run(taskSource, schema, i, output));
            }
            return reports;
        }
    }

    private ConfigSource getConfigWithDefaultValues()
    {
        return Exec.newConfigSource()
                .set("path", path.toString())
                .set("config", hdfsLocalFSConfig())
                .set("skip_header_lines", 1)
                .set("parser", parserConfig(schemaConfig()));
    }

    static List<TaskReport> emptyTaskReports(int taskCount)
    {
        ImmutableList.Builder<TaskReport> reports = new ImmutableList.Builder<>();
        for (int i = 0; i < taskCount; i++) {
            reports.add(Exec.newTaskReport());
        }
        return reports.build();
    }

    private ImmutableMap<String, Object> hdfsLocalFSConfig()
    {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put("fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem");
        builder.put("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        builder.put("fs.defaultFS", "file:///");
        return builder.build();
    }

    private ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig)
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("newline", "CRLF");
        builder.put("delimiter", ",");
        builder.put("quote", "\"");
        builder.put("escape", "\"");
        builder.put("trim_if_not_quoted", false);
        builder.put("skip_header_lines", 1);
        builder.put("allow_extra_columns", false);
        builder.put("allow_optional_columns", false);
        builder.put("columns", schemaConfig);
        return builder.build();
    }

    private ImmutableList<Object> schemaConfig()
    {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "id", "type", "long"));
        builder.add(ImmutableMap.of("name", "account", "type", "long"));
        builder.add(ImmutableMap.of("name", "time", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "purchase", "type", "timestamp", "format", "%Y%m%d"));
        builder.add(ImmutableMap.of("name", "comment", "type", "string"));
        return builder.build();
    }

    private void assertRecords(ConfigSource config, MockPageOutput output, long size)
    {
        List<Object[]> records = getRecords(config, output);
        assertEquals(size, records.size());
        {
            Object[] record = records.get(0);
            assertEquals(1L, record[0]);
            assertEquals(32864L, record[1]);
            assertEquals("2015-01-27 19:23:49 UTC", record[2].toString());
            assertEquals("2015-01-27 00:00:00 UTC", record[3].toString());
            assertEquals("embulk", record[4]);
        }

        {
            Object[] record = records.get(1);
            assertEquals(2L, record[0]);
            assertEquals(14824L, record[1]);
            assertEquals("2015-01-27 19:01:23 UTC", record[2].toString());
            assertEquals("2015-01-27 00:00:00 UTC", record[3].toString());
            assertEquals("embulk jruby", record[4]);
        }
    }

    private List<Object[]> getRecords(ConfigSource config, MockPageOutput output)
    {
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        return Pages.toObjects(schema, output.pages);
    }
}
