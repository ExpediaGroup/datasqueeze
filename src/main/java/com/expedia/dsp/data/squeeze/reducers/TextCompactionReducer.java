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
