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

import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests for {@link SeqCompactionMapper}
 *
 * @author Sai Sharan
 */


@RunWith(PowerMockRunner.class)
@PrepareForTest({ SeqCompactionMapper.class, FileSystem.class })
public class SeqCompactionMapperTest {

    private final Text keyText = new Text("keyText");
    private final Text text = new Text("value");
    private final TestMapperWrapper mapper = new TestMapperWrapper();
    private final Mapper.Context context = mock(Mapper.Context.class);
    private final Configuration configuration = mock(Configuration.class);
    private final FileSplit fileSplit = mock(FileSplit.class);
    private final FileSystem fileSystem = mock(FileSystem.class);
    private final FileStatus fileStatus = mock(FileStatus.class);

    @Before
    public void setup() throws IOException {
        PowerMockito.mockStatic(FileSystem.class);

        when(context.getConfiguration()).thenReturn(configuration);
        when(configuration.get("compactionThreshold")).thenReturn("1000");
        when(configuration.get("compactionDataSkew")).thenReturn(null);
        when(context.getInputSplit()).thenReturn(fileSplit);
        final Path path = new Path("/source/path/");
        when(fileSplit.getPath()).thenReturn(path);
        when(FileSystem.get(any(URI.class), any(Configuration.class))).thenReturn(fileSystem);
        FileStatus[] fileStatuses = { fileStatus };
        when(fileSystem.listStatus(any(Path.class))).thenReturn(fileStatuses);
        when(fileStatus.isDirectory()).thenReturn(false);
        when(fileStatus.getLen()).thenReturn(1234L);
        when(fileStatus.getPath()).thenReturn(path);
    }

    @Test
    public void testMap() throws Exception {
        when(configuration.get("compactionThreshold")).thenReturn("12345");
        mapper.map(keyText, text, context);
        Mockito.verify(context, Mockito.times(1)).write(Mockito.eq(new Text("/source/")), Matchers.anyObject());
    }

    @Test
    public void testMapThreshold() throws Exception {
        when(configuration.get("compactionThreshold")).thenReturn("0");
        mapper.map(keyText, text, context);
        Mockito.verify(context, Mockito.times(1)).write(Mockito.eq(new Text("/source/path")), Matchers.anyObject());
    }

    @Test
    public void testMapWithSkew() throws Exception {
        final String source = "/source/path";
        final JSONObject object = new JSONObject();
        object.put(source, 1);
        when(configuration.get("compactionDataSkew")).thenReturn(object.toString());
        mapper.map(keyText, text, context);
        Mockito.verify(context, Mockito.times(1)).write(Mockito.eq(new Text(source)), Matchers.anyObject());
    }

    public class TestMapperWrapper extends SeqCompactionMapper {

        @Override
        protected void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            setup(context);
            super.map(key, value, context);
        }
    }
}
