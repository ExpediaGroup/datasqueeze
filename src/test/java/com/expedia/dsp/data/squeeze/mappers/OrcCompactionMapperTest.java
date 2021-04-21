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
package com.expedia.dsp.data.squeeze.mappers;


import com.expedia.dsp.data.squeeze.impl.CombineFileWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.URI;

import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;


/**
 * Tests for {@link OrcCompactionMapper}
 *
 * @author Yashraj R. Sontakke
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({OrcCompactionMapper.class, OrcStruct.class, OrcValue.class, FileSystem.class})
public class OrcCompactionMapperTest {

    private final TypeDescription typeDescription = TypeDescription.createStruct().addField("field1", TypeDescription.createInt());
    private final IntWritable intWritable = new IntWritable(1);
    private final OrcStruct orcStruct = (OrcStruct) OrcStruct.createValue(typeDescription);
    private final TestMapperWrapper mapper = new TestMapperWrapper();
    private final Mapper.Context context = mock(Mapper.Context.class);
    private final Configuration configuration = mock(Configuration.class);
    private final FileSystem fileSystem = mock(FileSystem.class);
    private final FileStatus fileStatus = mock(FileStatus.class);
    private final CombineFileWritable combineFileWritable = new CombineFileWritable(0L, "/source/path/");

    @Before
    public void setup() throws IOException {
        PowerMockito.mockStatic(FileSystem.class);

        orcStruct.setFieldValue("field1", intWritable);
        when(context.getConfiguration()).thenReturn(configuration);
        when(configuration.get("compactionThreshold")).thenReturn("1000");
        when(configuration.get("compactionDataSkew")).thenReturn(null);
        final Path path = new Path("/source/path/");
        when(FileSystem.get(any(URI.class), any(Configuration.class))).thenReturn(fileSystem);
        FileStatus[] fileStatuses = {fileStatus};
        when(fileSystem.listStatus(any(Path.class))).thenReturn(fileStatuses);
        when(fileStatus.isDirectory()).thenReturn(false);
        when(fileStatus.getLen()).thenReturn(1234L);
        when(fileStatus.getPath()).thenReturn(path);
    }

    @Test
    public void testMap() throws Exception {
        when(configuration.get("compactionThreshold")).thenReturn("12345");
        mapper.map(combineFileWritable, orcStruct, context);
        Mockito.verify(context, Mockito.times(1)).write(Mockito.eq(new Text("/source/")), Matchers.anyObject());
    }

    @Test
    public void testMapThreshold() throws Exception {
        when(configuration.get("compactionThreshold")).thenReturn("0");
        mapper.map(combineFileWritable, orcStruct, context);
        Mockito.verify(context, Mockito.times(1)).write(Mockito.eq(new Text("/source/path")), Matchers.anyObject());
    }

    @Test
    public void testMapWithSkew() throws Exception {
        final String source = "/source/path";
        final JSONObject object = new JSONObject();
        object.put(source, 1);
        when(configuration.get("compactionDataSkew")).thenReturn(object.toString());
        mapper.map(combineFileWritable, orcStruct, context);
        Mockito.verify(context, Mockito.times(1)).write(Mockito.eq(new Text(source)), Matchers.anyObject());
    }


    public class TestMapperWrapper extends OrcCompactionMapper {

        protected void map(final CombineFileWritable key, final OrcStruct value, final Context context) throws IOException, InterruptedException {
            setup(context);
            super.map(key, value, context);
        }
    }
}
