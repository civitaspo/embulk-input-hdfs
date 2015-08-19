Embulk::JavaPlugin.register_input(
  "hdfs", "org.embulk.input.hdfs.HdfsFileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
