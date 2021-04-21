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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Tests for {@link BytesWritableCompactionReducer}
 *
 * @author Yashraj R. Sontakke
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BytesWritableCompactionReducer.class})
public class BytesWritableCompactionReducerTest {

    private final BytesWritableCompactionReducer reducer = new BytesWritableCompactionReducer();
    private final ReduceDriver<Text, BytesWritable, NullWritable, BytesWritable> reduceDriver = ReduceDriver.newReduceDriver(reducer);
    private final Text text1 = new Text("value1");
    private final Text text2 = new Text("value2");
    private final BytesWritable value1 = new BytesWritable(text1.getBytes());
    private final BytesWritable value2 = new BytesWritable(text2.getBytes());
    private final ArrayList<BytesWritable> values = new ArrayList<BytesWritable>();
    private final Text inputParentKey = new Text("value/src/path");
    private final Text inputFileKey = new Text("value/src/path/file");
    private final Configuration configuration = reduceDriver.getConfiguration();


    @Test
    public void testReduceParentKey() throws IOException {
        configuration.set("compactionSourcePath", "/src/path");
        configuration.set("compactionTargetPath", "/target/path");
        values.add(value1);
        values.add(value2);
        reduceDriver.withInput(inputParentKey, values);
        reduceDriver.withPathOutput(NullWritable.get(), value1, "value/target/path");
        reduceDriver.withPathOutput(NullWritable.get(), value2, "value/target/path");
        reduceDriver.runTest();
    }

    @Test
    public void testReduceFileKey() throws IOException {
        configuration.set("compactionSourcePath", "/src/path");
        configuration.set("compactionTargetPath", "/target/path");
        values.add(value1);
        values.add(value2);
        reduceDriver.withInput(inputFileKey, values);
        reduceDriver.withPathOutput(NullWritable.get(), value1, "value/target/path/file");
        reduceDriver.withPathOutput(NullWritable.get(), value2, "value/target/path/file");
        reduceDriver.runTest();
    }
}
