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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * Tests for {@link TextCombineFileInputFormat}
 *
 * @author Yashraj R. Sontakke
 */
@RunWith(PowerMockRunner.class)
public class TextCombineFileInputFormatTest {
    private final JobContext context = mock(JobContext.class);
    private final CombineFileSplit inputSplit = mock(CombineFileSplit.class);
    private final TaskAttemptContext taskAttemptContext = mock(TaskAttemptContext.class);

    @Test
    public void testConstructor() {
        CombineFileInputFormat combineFileInputFormat = new TextCombineFileInputFormat();
        assertTrue(combineFileInputFormat instanceof TextCombineFileInputFormat);
    }

    @Test
    public void testCreateRecordReader() throws IOException {
        TextCombineFileInputFormat combineFileInputFormat = new TextCombineFileInputFormat();
        RecordReader recordReader =  combineFileInputFormat.createRecordReader(inputSplit, taskAttemptContext);
        assertTrue(recordReader instanceof CombineFileRecordReader);
    }

    @Test
    public void testIsSplitable() {
        TextCombineFileInputFormat combineFileInputFormat = new TextCombineFileInputFormat();
        assertFalse(combineFileInputFormat.isSplitable(context, new Path("/soruce/path")));
    }
}
