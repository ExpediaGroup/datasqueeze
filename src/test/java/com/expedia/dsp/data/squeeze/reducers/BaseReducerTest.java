package com.expedia.dsp.data.squeeze.reducers;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * Tests for {@link BaseReducer}
 *
 * @author Yashraj R. Sontakke
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BaseReducer.class})
public class BaseReducerTest {

    private final Reducer.Context context = mock(Reducer.Context.class);
    private final Configuration configuration = mock(Configuration.class);
    private final BaseReducer baseReducer = new BaseReducer();
    private final MultipleOutputs multipleOutputs = mock(MultipleOutputs.class);
    private final Text value1 = new Text("value1");
    private final Text value2 = new Text("value2");
    private final TypeDescription typeDescription = TypeDescription.createStruct().addField("field1", TypeDescription.createInt());
    private final OrcStruct orcStruct = (OrcStruct) OrcStruct.createValue(typeDescription);

    @Before
    public void setup() throws Exception {
        when(context.getConfiguration()).thenReturn(configuration);
        when(configuration.get("compactionSourcePath")).thenReturn("/source");
        when(configuration.get("compactionTargetPath")).thenReturn("/target");
    }

    @Test
    public void testReduceParentKey() throws IOException {
        final String filePath = "/source/path";
        final URI uri = URI.create("/target/path");
        assertEquals(uri, baseReducer.getReducerKey(new Text(filePath), context));
    }

    @Test
    public void testReduceFileKey() throws IOException {
        final String filePath = "/source/path/";
        final URI uri = URI.create("/target/path/file");
        assertEquals(uri, baseReducer.getReducerKey(new Text(filePath), context));
    }

    @Test
    public void testReduceWriteDataOrc() throws Exception {
        final OrcValue value = new OrcValue(orcStruct);
        final List<OrcValue> list = ImmutableList.of(value);
        final URI uri = URI.create("/source/path");
        whenNew(MultipleOutputs.class).withArguments(context).thenReturn(multipleOutputs);
        baseReducer.writeData(uri, list, context);
    }

    @Test
    public void testReduceWriteDataText() throws Exception {
        final List<Text> list = ImmutableList.of(value1, value2);
        final URI uri = URI.create("/source/path");
        whenNew(MultipleOutputs.class).withArguments(context).thenReturn(multipleOutputs);
        baseReducer.writeData(uri, list, context);
    }
}
