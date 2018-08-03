package com.expedia.edw.data.squeeze.reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
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
 * Tests for {@link TextCompactionReducer}
 *
 * @author Yashraj R. Sontakke
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({TextCompactionReducer.class})
public class TextCompactionReducerTest {

    private final Text value1 = new Text("value1");
    private final Text value2 = new Text("value2");
    private final BaseReducer<Text> baseReducer = mock(BaseReducer.class);
    private final Reducer.Context context = mock(Reducer.Context.class);
    private final List<Text> list = new ArrayList<Text>();
    private final TestReducerWrapper reducer = new TestReducerWrapper();

    @Test
    public void testReduceParentKey() throws Exception {
        list.add(value1);
        list.add(value2);
        whenNew(BaseReducer.class).withNoArguments().thenReturn(baseReducer);
        final URI uri = URI.create("/source/path");
        when(baseReducer.getReducerKey(new Text("/source/path"), context)).thenReturn(uri);
        baseReducer.writeData(uri, list, context);
        doNothing().when(baseReducer).writeData(uri, list, context);
        reducer.reduce(new Text("/source/path"), list, context);
    }

    @Test
    public void testReduceFileKey() throws Exception {
        list.add(value1);
        list.add(value2);
        whenNew(BaseReducer.class).withNoArguments().thenReturn(baseReducer);
        final URI uri = URI.create("/source/path/");
        when(baseReducer.getReducerKey(new Text("/source/path/"), context)).thenReturn(uri);
        baseReducer.writeData(uri, list, context);
        doNothing().when(baseReducer).writeData(uri, list, context);
        reducer.reduce(new Text("/source/path/"), list, context);
    }

    public class TestReducerWrapper extends TextCompactionReducer {

        protected void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }
}
