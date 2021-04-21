/**
 * Copyright (C) 2017-2021 Expedia, Inc.
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
package com.expedia.dsp.data.squeeze.reducers;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.net.URI;

/**
 * Base Reducer class for retrieving the reducer key.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class BaseReducer<T> {

    /**
     * Retrieves the reducer key.
     *
     * @param key     the input {@link Text} key
     * @param context the {@link Context}
     * @return the reducer key as {@link URI}
     */
    protected URI getReducerKey(final Text key, final Context context) {
        final Configuration configuration = context.getConfiguration();
        final String sourcePath = configuration.get("compactionSourcePath");
        final String targetPath = configuration.get("compactionTargetPath");

        // Reducer stores data at the target directory retaining the directory structure of files
        String filePath = key.toString().replace(sourcePath, targetPath);
        if (key.toString().endsWith("/")) {
            filePath = filePath.concat("file");
        }

        log.info("Compaction output path {}", filePath);
        return URI.create(filePath);
    }

    /**
     * Writes data to the uri key provided
     *
     * @param uri     the {@link URI}. Example:- /dir/folder/file1
     * @param values  the {@link Iterable} values
     * @param context the {@link Context}
     * @throws IOException
     * @throws InterruptedException
     */
    protected void writeData(final URI uri, final Iterable<T> values, final Context context) throws IOException, InterruptedException {
        final MultipleOutputs multipleOutputs = new MultipleOutputs(context);
        try {
            for (final T value : values) {
                multipleOutputs.write(NullWritable.get(), value, uri.toString());
            }
        } finally {
            multipleOutputs.close();
        }
    }
}
