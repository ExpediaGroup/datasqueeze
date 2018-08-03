package com.expedia.edw.data.squeeze.reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
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
    private final OrcStruct orcStruct = (OrcStruct) OrcStruct.createValue(typeDescription);
    private final TestReducerWrapper reducer = new TestReducerWrapper();
    private final Reducer.Context context = mock(Reducer.Context.class);
    private final List<OrcValue> list = new ArrayList<OrcValue>();
    private final BaseReducer<OrcValue> baseReducer = mock(BaseReducer.class);

    @Before
    public void setup() throws Exception {
        final OrcValue value = new OrcValue(orcStruct);
        list.add(value);
    }

    @Test
    public void testReduceParentKey() throws Exception {
        whenNew(BaseReducer.class).withNoArguments().thenReturn(baseReducer);
        final URI uri = URI.create("/source/path");
        when(baseReducer.getReducerKey(new Text("/source/path"), context)).thenReturn(uri);
        baseReducer.writeData(uri, list, context);
        doNothing().when(baseReducer).writeData(uri, list, context);
        reducer.reduce(new Text("/source/path"), list, context);
    }


    @Test
    public void testReduceFileKey() throws Exception {
        whenNew(BaseReducer.class).withNoArguments().thenReturn(baseReducer);
        final URI uri = URI.create("/source/path/");
        when(baseReducer.getReducerKey(new Text("/source/path/"), context)).thenReturn(uri);
        baseReducer.writeData(uri, list, context);
        doNothing().when(baseReducer).writeData(uri, list, context);
        reducer.reduce(new Text("/source/path/"), list, context);
    }

    public class TestReducerWrapper extends OrcCompactionReducer {

        protected void reduce(final Text key, final Iterable<OrcValue> values, final Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }
}
