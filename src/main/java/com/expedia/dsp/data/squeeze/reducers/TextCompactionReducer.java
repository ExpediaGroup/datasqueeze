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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;

/**
 * Reducer class for Text and Sequence input file formats.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class TextCompactionReducer extends Reducer<Text, Text, NullWritable, Text> {

    /**
     * {@inheritDoc}
     */
    protected void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        final BaseReducer baseReducer = new BaseReducer();
        final URI uri = baseReducer.getReducerKey(key, context);
        baseReducer.writeData(uri, values, context);
    }
}
