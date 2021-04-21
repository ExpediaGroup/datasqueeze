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
package com.expedia.dsp.data.squeeze.impl.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.orc.OrcFile;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.InputStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * Tests for {@link TextRecordReader}
 *
 * @author Yashraj R. Sontakke
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({TextRecordReaderTest.class, FileSystem.class, LineReader.class})
public class TextRecordReaderTest {
    private final CombineFileSplit inputSplit = mock(CombineFileSplit.class);
    private final TaskAttemptContext taskAttemptContext = mock(TaskAttemptContext.class);
    private Configuration configuration = mock(Configuration.class);
    private final FileSystem fileSystem = mock(FileSystem.class);
    final Path path = new Path("source/path/text/textfile.txt");
    CompressionCodecFactory compressionCodecFactory = mock(CompressionCodecFactory.class);
    FSDataInputStream fileIn = mock(FSDataInputStream.class);
    LineReader reader = mock(LineReader.class);

    @Test
    public void testReader() throws Exception {
        PowerMockito.mockStatic(FileSystem.class);
        PowerMockito.mockStatic(OrcFile.ReaderOptions.class);
        PowerMockito.mockStatic(LineReader.class);

        when(inputSplit.getPath(0)).thenReturn(path);
        when(inputSplit.getOffset(0)).thenReturn(0L);
        when(inputSplit.getLength(0)).thenReturn(10L);
        when(taskAttemptContext.getConfiguration()).thenReturn(configuration);
        when(fileSystem.makeQualified(path)).thenReturn(path);
        when(path.getFileSystem(configuration)).thenReturn(fileSystem);

        whenNew(CompressionCodecFactory.class).withArguments(configuration).thenReturn(compressionCodecFactory);
        when(compressionCodecFactory.getCodec(path)).thenReturn(null);

        when(fileSystem.open(path)).thenReturn(fileIn);
        whenNew(LineReader.class).withArguments(any(InputStream.class), anyInt()).thenReturn(reader);
        when(reader.readLine(any(Text.class))).thenReturn(5);
        RecordReader recordReader = new TextRecordReader(inputSplit, taskAttemptContext, 0);
        assertNotNull(recordReader);
        assertFalse(recordReader.nextKeyValue());
    }
}
