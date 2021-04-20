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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for {@link BaseMapper}
 *
 * @author Yashraj R. Sontakke
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BaseMapper.class, FileSystem.class})
public class BaseMapperTest {

    public static final String SOURCE_PATH = "/source/path/";
    private final Context context = mock(Context.class);
    private final Configuration configuration = mock(Configuration.class);
    private final FileSystem fileSystem = mock(FileSystem.class);
    private final FileStatus fileStatus = mock(FileStatus.class);
    private BaseMapper baseMapper;

    @Before
    public void setup() throws Exception {
        PowerMockito.mockStatic(FileSystem.class);
        when(context.getConfiguration()).thenReturn(configuration);
        when(configuration.get("compactionThreshold")).thenReturn("1000");

        final Path path = new Path("/source/path/");
        URI uri = URI.create(path.toString());
        when(FileSystem.get(uri, configuration)).thenReturn(fileSystem);
        FileStatus[] fileStatuses = {fileStatus};
        when(fileSystem.listStatus(any(Path.class))).thenReturn(fileStatuses);
        when(fileStatus.isDirectory()).thenReturn(false);
        when(fileStatus.getLen()).thenReturn(234L);
        when(fileStatus.getPath()).thenReturn(path);
        baseMapper = new BaseMapper(context);

        when(configuration.get("compactionSourcePath")).thenReturn("/source");
        when(configuration.get("compactionTargetPath")).thenReturn("/target");
    }

    @Test
    public void testMapperParentKey() throws IOException {
        assertEquals(new Text("/source/"), baseMapper.getKey(SOURCE_PATH));
    }

    @Test
    public void testMapContainsKey() throws IOException {
        baseMapper.getKey(SOURCE_PATH);
        assertEquals(new Text("/source/"), baseMapper.getKey(SOURCE_PATH));
    }

    @Test
    public void testMapperFileKey() throws IOException {
        when(fileStatus.getLen()).thenReturn(1234L);
        assertEquals(new Text("/source/path"), baseMapper.getKey(SOURCE_PATH));
    }

    @Test
    public void testDataSkewKey() throws IOException {
        final String source = "/source/";
        final JSONObject object = new JSONObject();
        object.put(source, 1);
        when(configuration.get("compactionDataSkew")).thenReturn(object.toString());
        baseMapper = new BaseMapper(context);
        final Text key = baseMapper.getKey(SOURCE_PATH);
        assertNotNull(key);
        assertTrue(key.toString().contains("/source/compactedFile-"));
    }
}
