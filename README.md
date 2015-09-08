# Hdfs file input plugin for Embulk

Read files on Hdfs.

## Overview

* **Plugin type**: file input
* **Resume supported**: not yet
* **Cleanup supported**: no

## Configuration

- **config_files** list of paths to Hadoop's configuration files (array of strings, default: `[]`)
- **config** overwrites configuration parameters (hash, default: `{}`)
- **input_path** file path on Hdfs. you can use glob and Date format like `%Y%m%d/%s`.
- **rewind_seconds** When you use Date format in input_path property, the format is executed by using the time which is Now minus this property.
- **partition** when this is true, partition input files and increase task count. (default: `true`)

## Example

```yaml
in:
  type: hdfs
  config_files:
    - /opt/analytics/etc/hadoop/conf/core-site.xml
    - /opt/analytics/etc/hadoop/conf/hdfs-site.xml
  config:
    fs.defaultFS: 'hdfs://hadoop-nn1:8020'
    dfs.replication: 1
    fs.hdfs.impl: 'org.apache.hadoop.hdfs.DistributedFileSystem'
    fs.file.impl: 'org.apache.hadoop.fs.LocalFileSystem'
  input_path: /user/embulk/test/%Y-%m-%d/*
  rewind_seconds: 86400
  partition: true
  decoders:
    - {type: gzip}
  parser:
    charset: UTF-8
    newline: CRLF
    type: csv
    delimiter: "\t"
    quote: ''
    escape: ''
    trim_if_not_quoted: true
    skip_header_lines: 0
    allow_extra_columns: true
    allow_optional_columns: true
    columns:
    - {name: c0, type: string}
    - {name: c1, type: string}
    - {name: c2, type: string}
    - {name: c3, type: long}
```

## Note
- the feature of the partition supports only 3 line terminators.
  - `\n`
  - `\r`
  - `\r\n`

## The Reference Implementation
- [hito4t/embulk-input-filesplit](https://github.com/hito4t/embulk-input-filesplit)

## Build

```
$ ./gradlew gem
```

## Development

```
$ ./gradlew classpath
$ bundle exec embulk run -I lib example.yml
```