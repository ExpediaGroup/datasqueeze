package com.expedia.edw.data.squeeze.reducers;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests for {@link AvroCompactionReducer}
 *
 * @author Samarth Kulkarni
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ AvroCompactionReducer.class, FileSystem.class, AvroKey.class, AvroValue.class })
public class AvroCompactionReducerTest {

    private final Configuration configuration = mock(Configuration.class);
    private final Reducer.Context context = mock(Reducer.Context.class);
    private final TestReducerWrapper reducer = new TestReducerWrapper();
    private final FileSplit fileSplit = mock(FileSplit.class);
    private final FileStatus fileStatus = mock(FileStatus.class);
    private final FileSystem fileSystem = mock(FileSystem.class);
    private final AvroMultipleOutputs multipleOutputs = mock(AvroMultipleOutputs.class);
    private final List<AvroValue<GenericRecord>> list = new ArrayList<AvroValue<GenericRecord>>();
    private GenericRecord userRecord = null;

    @Before
    public void setup() throws Exception {
        String schemaJSON = "{" +
                "   \"namespace\": \"example.avro\"," +
                "   \"type\": \"record\"," +
                "   \"name\": \"User\"," +
                "   \"fields\": [" +
                "      {\"name\": \"name\", \"type\": \"string\"}," +
                "      {\"name\": \"favorite_number\",  \"type\": \"int\"}" +
                "   ] " +
                " }";
        Schema schema = new Schema.Parser().parse(schemaJSON);
        userRecord = new GenericData.Record(schema);
        userRecord.put("name", "Alyssa");
        userRecord.put("favorite_number", 256);

        final AvroValue<GenericRecord> avroValue = new AvroValue<GenericRecord>(userRecord);
        list.add(avroValue);

        when(context.getConfiguration()).thenReturn(configuration);
        when(configuration.get("compactionSourcePath")).thenReturn("/source");
        when(configuration.get("compactionTargetPath")).thenReturn("/target");
        whenNew(AvroMultipleOutputs.class).withArguments(context).thenReturn(multipleOutputs);
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        reducer.reduce(new Text("/source/path"), list, context);
        Mockito.verify(multipleOutputs, Mockito.times(1)).write(Matchers.eq(new AvroKey<GenericRecord>(userRecord)), Matchers.any(NullWritable.class), Mockito.eq("/target/path"));
    }

    public class TestReducerWrapper extends AvroCompactionReducer {
        @Override
        protected void reduce(final Text key, final Iterable<AvroValue<GenericRecord>> values, final Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }
}
