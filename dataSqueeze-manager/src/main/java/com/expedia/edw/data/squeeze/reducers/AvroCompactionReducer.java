package com.expedia.edw.data.squeeze.reducers;

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
