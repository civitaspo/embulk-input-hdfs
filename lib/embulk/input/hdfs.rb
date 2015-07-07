Embulk::JavaPlugin.register_input(
  "hdfs", "org.embulk.input.HdfsFileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
