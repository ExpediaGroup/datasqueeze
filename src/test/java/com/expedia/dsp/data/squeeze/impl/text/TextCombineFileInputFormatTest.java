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
