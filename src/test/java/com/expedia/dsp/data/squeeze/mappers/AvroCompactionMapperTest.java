package com.expedia.dsp.data.squeeze.mappers;

import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;
import java.net.URI;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests for {@link AvroCompactionMapper}
 *
 * @author Samarth Kulkarni
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ AvroCompactionMapper.class, FileSystem.class, AvroKey.class, AvroValue.class })
public class AvroCompactionMapperTest {

    private final Configuration configuration = mock(Configuration.class);
    private final Mapper.Context context = mock(Mapper.Context.class);
    private final FileSplit fileSplit = mock(FileSplit.class);
    private final FileStatus fileStatus = mock(FileStatus.class);
    private final FileSystem fileSystem = mock(FileSystem.class);
    private final TestMapperWrapper mapper = new TestMapperWrapper();
    private AvroKey<GenericRecord> avroKey = null;
    private AvroValue<GenericRecord> avroValue = null;
    private AvroKey<GenericRecord> incompatibleAvroKey = null;
    private MapDriver<AvroKey<GenericRecord>, Object, Text, AvroValue<GenericRecord>> mapDriver;

    @Before
    public void setup() throws IOException {
        PowerMockito.mockStatic(FileSystem.class);
        AvroCompactionMapper mapper = new AvroCompactionMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        when(context.getConfiguration()).thenReturn(configuration);
        when(configuration.get("compactionThreshold")).thenReturn("1000");
        when(context.getInputSplit()).thenReturn(fileSplit);
        final Path path = new Path("/source/path/");
        when(fileSplit.getPath()).thenReturn(path);
        when(FileSystem.get(any(URI.class), any(Configuration.class))).thenReturn(fileSystem);
        FileStatus[] fileStatuses = { fileStatus };
        when(fileSystem.listStatus(any(Path.class))).thenReturn(fileStatuses);
        when(fileStatus.isDirectory()).thenReturn(false);
        when(fileStatus.getPath()).thenReturn(path);

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
        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);

        String incompatibleJSON = "{" +
                "   \"namespace\": \"example.avro\"," +
                "   \"type\": \"record\"," +
                "   \"name\": \"User\"," +
                "   \"fields\": [" +
                "      {\"name\": \"EID\", \"type\": \"string\"}," +
                "      {\"name\": \"Vehicle\",  \"type\": \"int\"}" +
                "   ] " +
                " }";
        Schema incompatibleschema = new Schema.Parser().parse(incompatibleJSON);
        GenericRecord user2 = new GenericData.Record(incompatibleschema);
        user2.put("EID", "E101");
        user2.put("Vehicle", 2);

        avroKey = new AvroKey<GenericRecord>(user1);
        avroValue = new AvroValue<GenericRecord>(user1);
        incompatibleAvroKey = new AvroKey<GenericRecord>(user2);
    }

    @Test
    public void testMapBelowThreshold() throws IOException, InterruptedException {
        when(fileStatus.getLen()).thenReturn(123L);
        mapper.map(avroKey, NullWritable.get(), context);
        Mockito.verify(context, Mockito.times(1)).write(Mockito.eq(new Text("/source/")), Mockito.eq(avroValue));
    }
    @Test
    public void testMapAboveThreshold() throws IOException, InterruptedException {
        when(fileStatus.getLen()).thenReturn(1200L);
        mapper.map(avroKey, NullWritable.get(), context);
        Mockito.verify(context, Mockito.times(1)).write(Mockito.eq(new Text("/source/path")), Mockito.eq(avroValue));
    }

    @Test
    public void testMapInCompatibleData() throws IOException, InterruptedException {
        when(fileStatus.getLen()).thenReturn(123L);
        mapper.map(incompatibleAvroKey, NullWritable.get(), context);
        Mockito.verify(context, Mockito.times(0)).write(Mockito.eq(new Text("/source/")), Mockito.eq(avroValue));
    }

    public class TestMapperWrapper extends AvroCompactionMapper {
        @Override
        protected void map(final AvroKey<GenericRecord> key, final Object value, final Context context) throws IOException, InterruptedException {
            setup(context);
            super.map(key, value, context);
        }
    }
}
