<p align="center">
  <img src="dataSqueeze-logo.png">
</p>

## Overview
DataSqueeze performs compaction of files from source directory to target directory maintaining the directory structure of the source.

## Documentation
 This README is intended to provide detailed technical documentation for advanced users.

## Changes since last release

* Edited the pom file for publishing this project's artifacts to the Maven Central
* Changed namespace to com.expedia.dsp
* Renamed dataSqueeze to datasqueeze
* Refactored code to remove the dataSqueeze-manager layer
* Added DataSqueeze logo

## General operation

DataSqueeze supports two types of compaction

1. Normal Compaction - We compact files from source to target path.

    Below is a high level summary of the steps that Compaction Utility performs during the course of a typical run for normal compaction.

        a. Fetch the source file paths to be compacted from the source path provided.
        b. Perform mapreduce job using the following configuration
            1. Mapper maps records together based on same parent directory and emits parent directory as key.
            2. Reducer reduces records based on same key but writes data to the target directory provided by the user, 
               retaining the directory structure.

2. In-Place Compaction - Performs compaction on the source path. This is not recommended on AWS-S3, since the performance will be terrible.

    Below is a high level summary of the steps that Compaction Utility performs during the course of a typical run for in-place compaction.


        a. Fetch the file paths to be compacted from the source path provided.
        b. Perform mapreduce job using the following configuration
            1. Mapper maps records together based on same parent directory and emits parent directory as key.
            2. Reducer reduces records based on same key but writes data to the target directory provided by the user, 
               retaining the directory structure.
        c. Store the compacted files on temp-compacted path.
        d. Move files from source to temp location.
        e. Move files from temp-compacted location to source location specified by the user.


## Requirements

* MacOS or Linux
* Java 7 or later
* Maven 3.x (for building)
* rpmbuild (for building RPMs)

## Building DataSqueeze

DataSqueeze is a standard Maven project. Run the following in the project root folder:

    mvn clean package

The compiled JAR can be found at `datasqueeze/target/datasqueeze.jar`.

To build an RPM, use the optional Maven profile `-P rpm`:

    mvn clean package -P rpm

This requires `rpmbuild` to be installed, otherwise an error will occur.

## Running DataSqueeze

There are two different ways of running DataSqueeze:

1. CLI -
    a. For TEXT/ORC/SEQ
    ```java
        hadoop jar datasqueeze.jar com.expedia.dsp.data.squeeze.Utility
        -sp s3a://edwprod/user/ysontakke/compactiontest1/ -tp s3a://edwprod/user/ysontakke/compactionoutput_text_yash_1/
        -threshold 12345
    ```

    b. For AVRO
    ```java
        hadoop jar datasqueeze.jar com.expedia.dsp.data.squeeze.Utility
        -sp s3a://edwprod/user/ysontakke/compactiontest1/ -tp s3a://edwprod/user/ysontakke/compactionoutput_text_yash_1/
        -threshold 12345 -fileType AVRO -schemaPath s3a://edwprod/user/ysontakke/compactionschema_text_yash_1/schema.avsc
    ```


    CLI uses four parameters:-

       * sp (SourcePath) - Source location for compaction
       * tp (TargetPath) - Target location for compaction. If target path is not provided, inplace compaction is performed
       * threshold - Optional field. threshold in bytes for compaction. If file size is greater then no compaction on file,
         file is just copied to target directory. Optional parameter, if not provided defaults to 134217728 (128 MB)
       * maxReducers - Max reducers for the Map Reduce job
       * fileType - Type of file to be compacted (AVRO / TEXT / SEQ / ORC). It is mandatory for AVRO
       * schemaPath - schema used for compaction (mandatory for AVRO)

2. API - [CompactionManager](datasqueeze/blob/master/src/main/java/com/expedia/dsp/data/squeeze/CompactionManager.java)

    ```java
        CompactionResponse compact() throws Exception;
    ```

## Tests

Currently, the tests for DataSqueeze cannot be made publicly available, but we are working on getting them open sourced.

## Contributing

We gladly accept contributions to DataSqueeze in the form of issues, feature requests, and pull requests!

## Licensing

Copyright © 2017 Expedia, Inc.

DataSqueeze is licensed under the Apache 2.0 license; refer to [LICENSE](LICENSE) for the complete text.
