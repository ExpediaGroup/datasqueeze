/**
 * Copyright (C) 2018 Expedia Group
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
package com.expedia.dsp.data.squeeze.impl.orc;

import com.expedia.dsp.data.squeeze.impl.CombineFileWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.mapred.OrcStruct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * Tests for {@link OrcCombineFileInputFormat}
 *
 * @author Yashraj R. Sontakke
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({OrcRecordReaderTest.class, FileSystem.class, ReaderImpl.class, OrcTail.class, OrcFile.ReaderOptions.class, OrcFile.class, ReaderImpl.class, org.apache.orc.RecordReader.class})
public class OrcRecordReaderTest {
    private final CombineFileSplit inputSplit = mock(CombineFileSplit.class);
    private final TaskAttemptContext taskAttemptContext = mock(TaskAttemptContext.class);
    private Configuration configuration = mock(Configuration.class);
    private ReaderImpl reader = mock(ReaderImpl.class);
    private OrcFile.ReaderOptions readerOptions = mock(OrcFile.ReaderOptions.class);
    private final FileSystem fileSystem = mock(FileSystem.class);
    private OrcTail orcTail = mock(OrcTail.class);
    final Path path = new Path("/source/path");
    org.apache.orc.RecordReader in = mock(org.apache.orc.RecordReader.class);
    private final TypeDescription typeDescription = TypeDescription.createStruct().addField("field1", TypeDescription.createInt());

    @Test
    public void testReader() throws Exception {
        PowerMockito.mockStatic(FileSystem.class);
        PowerMockito.mockStatic(OrcFile.ReaderOptions.class);
        PowerMockito.mockStatic(OrcFile.class);

        when(inputSplit.getPath(0)).thenReturn(path);
        when(inputSplit.getOffset(0)).thenReturn(0L);
        when(inputSplit.getLength(0)).thenReturn(10L);
        when(taskAttemptContext.getConfiguration()).thenReturn(configuration);
        when(fileSystem.makeQualified(path)).thenReturn(path);
        when(path.getFileSystem(configuration)).thenReturn(fileSystem);

        whenNew(OrcFile.ReaderOptions.class).withAnyArguments().thenReturn(readerOptions);
        when(OrcFile.readerOptions(configuration)).thenReturn(readerOptions);
        when(readerOptions.getFilesystem()).thenReturn(fileSystem);
        when(readerOptions.getOrcTail()).thenReturn(orcTail);
        when(OrcFile.createReader(path, readerOptions)).thenReturn(reader);
        whenNew(ReaderImpl.class).withArguments(path, readerOptions).thenReturn(reader);

        when(reader.rows(any(Reader.Options.class))).thenReturn(in);
        when(reader.getSchema()).thenReturn(typeDescription);

        RecordReader recordReader = new OrcRecordReader(inputSplit, taskAttemptContext, 0);
        assertFalse(recordReader.nextKeyValue());
        assertNotNull(recordReader.getCurrentKey());
        CombineFileWritable combineFileWritable = (CombineFileWritable) recordReader.getCurrentKey();
        assertEquals(path.toString(), combineFileWritable.getFileName());

        OrcStruct orcStruct = (OrcStruct) OrcStruct.createValue(typeDescription);
        assertEquals(orcStruct, recordReader.getCurrentValue());
    }

}
