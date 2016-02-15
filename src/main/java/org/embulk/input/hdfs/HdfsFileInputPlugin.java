package org.embulk.input.hdfs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.hdfs.util.Hdfs;
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
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HdfsFileInputPlugin
        implements FileInputPlugin
{
    private static final Logger logger = Exec.getLogger(HdfsFileInputPlugin.class);

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

        @Config("use_compression_codec") // if true, use compression codec when getting FileInputStream.
        @ConfigDefault("false")
        boolean getUseCompressionCodec();

        List<HdfsPartialFile> getFiles();
        void setFiles(List<HdfsPartialFile> hdfsFiles);

        @ConfigInject
        ScriptingContainer getJRuby();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        Hdfs hdfs = new Hdfs(task);

        // listing Files
        String pathString = strftime(task, task.getPath(), task.getRewindSeconds());
        try {
            List<String> originalFileList = buildFileList(hdfs.getFS(), pathString);

            if (originalFileList.isEmpty()) {
                throw new PathNotFoundException(pathString);
            }

            logger.debug("embulk-input-hdfs: Loading target files: {}", originalFileList);
            task.setFiles(allocateHdfsFilesToTasks(task, hdfs, originalFileList));
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

    @VisibleForTesting
    String strftime(final PluginTask task, final String format, final int rewindSeconds)
    {
        String script = String.format("(Time.now - %d).strftime('%s')", rewindSeconds, format);
        return task.getJRuby().runScriptlet(script).toString();
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
                fileList.addAll(listRecursive(fs, entry));
            }
            else {
                fileList.add(entry.getPath().toString());
            }
        }

        return fileList;
    }

    private List<String> listRecursive(final FileSystem fs, FileStatus status)
            throws IOException
    {
        List<String> fileList = new ArrayList<>();
        if (status.isDirectory()) {
            for (FileStatus entry : fs.listStatus(status.getPath())) {
                fileList.addAll(listRecursive(fs, entry));
            }
        }
        else {
            fileList.add(status.getPath().toString());
        }
        return fileList;
    }

    private List<HdfsPartialFile> allocateHdfsFilesToTasks(PluginTask task, Hdfs hdfs, List<String> fileList)
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

        ImmutableMap.Builder<Path, Long> pathLongBuilder = ImmutableMap.builder();
        long totalFileLength = 0;
        for (Path path : pathList) {
            long fileLength = 0;
            Optional<CompressionCodec> codec = hdfs.getCompressionCodec(path);
            if (codec.isPresent() && task.getUseCompressionCodec()) {
                InputStream is = codec.get().createInputStream(hdfs.getFS().open(path));
//                fileLength += is.available();
                while (is.read() > 0) {
                    fileLength++;
                }
                is.close();
                logger.info("`embulk-input-hdfs`: file: {}, length: {}, codec: {}", path, fileLength, codec.get().toString());
            }
            else {
                fileLength = hdfs.getFS().getFileStatus(path).getLen();
                logger.info("`embulk-input-hdfs`: file: {}, length: {}, codec: null", path, fileLength);
            }

            if (fileLength <= 0) {
                logger.info("`embulk-input-hdfs`: Skip the 0 byte target file: {}", path);
                continue;
            }

            pathLongBuilder.put(path, fileLength);
            totalFileLength += fileLength;
        }
        if (totalFileLength <= 0) {
            throw new PathIOException(task.getPath(), "`embulk-input-hdfs`: data is empty");
        }

        // TODO: optimum allocation of resources
        long approximateNumPartitions =
                (task.getApproximateNumPartitions() <= 0) ? Runtime.getRuntime().availableProcessors() : task.getApproximateNumPartitions();
        long partitionSizeByOneTask = totalFileLength / approximateNumPartitions;
        if (partitionSizeByOneTask <= 0) {
            partitionSizeByOneTask = 1;
        }

        ImmutableList.Builder<HdfsPartialFile> hdfsPartialFileBuilder = ImmutableList.builder();
        for (Map.Entry<Path, Long> entry : pathLongBuilder.build().entrySet()) {
            Path path = entry.getKey();
            long fileLength = entry.getValue();
            Optional<CompressionCodec> codec = hdfs.getCompressionCodec(path);

            long numPartitions;
            if (task.getPartition()) {
                if (codec.isPresent() && task.getUseCompressionCodec()) {
                    numPartitions = ((fileLength - 1) / partitionSizeByOneTask) + 1;
                }
                else if (path.toString().endsWith(".gz") || path.toString().endsWith(".bz2") || path.toString().endsWith(".lzo")) {
                    numPartitions = 1;
                }
                else {
                    numPartitions = ((fileLength - 1) / partitionSizeByOneTask) + 1;
                }
            }
            else {
                numPartitions = 1;
            }

            HdfsFilePartitioner partitioner = new HdfsFilePartitioner(hdfs.getFS(), path, numPartitions, fileLength);
            hdfsPartialFileBuilder.addAll(partitioner.getHdfsPartialFiles());
        }

        return hdfsPartialFileBuilder.build();
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
        final Hdfs hdfs = new Hdfs(task);

        InputStream input;
        final HdfsPartialFile file = task.getFiles().get(taskIndex);
        try {
            if (file.getStart() > 0 && task.getSkipHeaderLines() > 0) {
                input = new SequenceInputStream(getHeadersInputStream(task, hdfs, file), openInputStream(task, hdfs, file));
            }
            else {
                input = openInputStream(task, hdfs, file);
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

    private InputStream getHeadersInputStream(PluginTask task, Hdfs hdfs, HdfsPartialFile partialFile)
            throws IOException
    {
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        int skippedHeaders = 0;
        final InputStream hdfsFileInputStream;
        Path hdfsFile = new Path(partialFile.getPath());
        Optional<CompressionCodec> codec = hdfs.getCompressionCodec(hdfsFile);
        if (codec.isPresent()) {
            if (!task.getUseCompressionCodec()) {
                throw new ConfigException("`skip_header_line` must be with `use_compression_codec` option");
            }
            hdfsFileInputStream = codec.get().createInputStream(hdfs.getFS().open(hdfsFile));
        }
        else {
            hdfsFileInputStream = hdfs.getFS().open(hdfsFile);
        }

        try (BufferedInputStream in = new BufferedInputStream(hdfsFileInputStream)) {
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

    private HdfsPartialFileInputStream openInputStream(PluginTask task, Hdfs hdfs, HdfsPartialFile partialFile)
            throws IOException
    {
        Path path = new Path(partialFile.getPath());
        final InputStream original;
        Optional<CompressionCodec> codec = hdfs.getCompressionCodec(path);
        if (codec.isPresent()) {
            original = codec.get().createInputStream(hdfs.getFS().open(path));
        }
        else {
            original = hdfs.getFS().open(path);
        }
        return new HdfsPartialFileInputStream(original, partialFile.getStart(), partialFile.getEnd());
    }
}
