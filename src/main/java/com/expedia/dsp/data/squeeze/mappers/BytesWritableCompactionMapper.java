/**
 * Copyright (C) 2018 Expedia Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expedia.dsp.data.squeeze.mappers;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Mapper class for Sequence input file format with BytesWritable Value format.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class BytesWritableCompactionMapper extends Mapper<Object, BytesWritable, Text, BytesWritable> {

    final Map<String, Long> fileSizesMap = new HashMap<String, Long>();
    private FileSystem fileSystem;
    private Configuration configuration;
    long threshold;

    /**
     * {@inheritDoc}
     */
    protected void setup(Context context) throws IOException, InterruptedException {
        configuration = context.getConfiguration();
        final String compactionThreshold = configuration.get("compactionThreshold");
        threshold = Long.parseLong(compactionThreshold);
        log.info("Compaction threshold {}", threshold);
    }

    /**
     * {@inheritDoc}
     */
    protected void map(final Object key, final BytesWritable value, final Context context) throws IOException, InterruptedException {
        if (value!= null && value.toString() != null && value.toString().isEmpty()) {
            return;
        }

        // Mapper sends data with parent directory path as keys to retain directory structure
        final FileSplit fileSplit = (FileSplit) context.getInputSplit();
        final Path filePath = fileSplit.getPath();
        final String parentFilePath = String.format("%s/", filePath.getParent().toString());
        log.debug("Parent file path {}", parentFilePath);

        if (!fileSizesMap.containsKey(filePath.toString())) {
            if (fileSystem == null){
                final URI uri = URI.create(filePath.toString());
                fileSystem = FileSystem.get(uri, configuration);
            }
            final FileStatus[] listStatuses = fileSystem.listStatus(filePath);
            for (FileStatus fileStatus : listStatuses) {
                if (!fileStatus.isDirectory()) {
                    fileSizesMap.put(fileStatus.getPath().toString(), fileStatus.getLen());
                    log.info("Entry added to fileSizes Map {} {}", fileStatus.getPath().toString(), fileStatus.getLen());
                }
            }
        }

        final Text parentFilePathKey = new Text(parentFilePath);
        final Text filePathKey = new Text(filePath.toString());
        final Long fileSize = fileSizesMap.get(filePath.toString());
        if (fileSize < threshold) {
            context.write(parentFilePathKey, value);
        } else {
            context.write(filePathKey, value);
        }
    }
}
