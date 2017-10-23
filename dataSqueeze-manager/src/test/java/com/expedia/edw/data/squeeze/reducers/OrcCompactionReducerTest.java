package com.expedia.edw.data.squeeze.reducers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.powermock.api.mockito.PowerMockito.*;

/**
 * Tests for {@link OrcCompactionReducer}
 *
 * @author Yashraj R. Sontakke
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({OrcCompactionReducer.class})
public class OrcCompactionReducerTest {
    private final TypeDescription typeDescription = TypeDescription.createStruct().addField("field1", TypeDescription.createInt());
    private final IntWritable intWritable = new IntWritable(1);
    private final OrcStruct orcStruct = (OrcStruct) OrcStruct.createValue(typeDescription);
    private final TestMapperWrapper mapper = new TestMapperWrapper();
    private final Reducer.Context context = mock(Reducer.Context.class);
    private final Configuration configuration = mock(Configuration.class);
    private final MultipleOutputs multipleOutputs = mock(MultipleOutputs.class);
    private final List<OrcValue> list = new ArrayList<OrcValue>();

    @Before
    public void setup() throws Exception {
        orcStruct.setFieldValue("field1", intWritable);
        when(context.getConfiguration()).thenReturn(configuration);
        when(configuration.get("compactionSourcePath")).thenReturn("/source");
        when(configuration.get("compactionTargetPath")).thenReturn("/target");
        whenNew(MultipleOutputs.class).withArguments(context).thenReturn(multipleOutputs);
        final OrcValue value = new OrcValue(orcStruct);
        list.add(value);
    }

    @Test
    public void testReduceParentKey() throws Exception {
        mapper.reduce(new Text("/source/path"), list, context);
        Mockito.verify(multipleOutputs, Mockito.times(1)).write(Matchers.any(NullWritable.class), Matchers.any(OrcValue.class), Mockito.eq("/target/path"));
    }


    @Test
    public void testReduceFileKey() throws Exception {
        mapper.reduce(new Text("/source/path/"), list, context);
        Mockito.verify(multipleOutputs, Mockito.times(1)).write(Matchers.any(NullWritable.class), Matchers.any(OrcValue.class), Mockito.eq("/target/path/file"));
    }

    public class TestMapperWrapper extends OrcCompactionReducer {

        protected void reduce(final Text key, final Iterable<OrcValue> values, final Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }
}
