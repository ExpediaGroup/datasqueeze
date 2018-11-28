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
package com.expedia.dsp.data.squeeze.reducers;

import java.io.IOException;
import java.net.URI;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import lombok.extern.slf4j.Slf4j;

/**
 * Reducer class for AVRO input file format.
 *
 * @author Samarth Kulkarni
 */
@Slf4j
public class AvroCompactionReducer extends Reducer<Text, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {
    @Override
    protected void reduce(final Text key, final Iterable<AvroValue<GenericRecord>> values, final Context context) throws IOException, InterruptedException {
        final BaseReducer baseReducer = new BaseReducer();
        final URI uri = baseReducer.getReducerKey(key, context);

        final AvroMultipleOutputs multipleOutputs = new AvroMultipleOutputs(context);
        try {
            for (AvroValue value : values) {
                GenericRecord rec = (GenericRecord) value.datum();
                multipleOutputs.write(new AvroKey<GenericRecord>(rec), NullWritable.get(), uri.toString());

            }
        } finally {
            multipleOutputs.close();
        }
    }
}
