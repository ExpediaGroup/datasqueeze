package com.expedia.edw.data.squeeze.impl.orc;

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
 * Tests for {@link OrcCombineFileInputFormat}
 *
 * @author Yashraj R. Sontakke
 */
@RunWith(PowerMockRunner.class)
public class OrcCombineFileInputFormatTest {
    private final JobContext context = mock(JobContext.class);
    private final CombineFileSplit inputSplit = mock(CombineFileSplit.class);
    private final TaskAttemptContext taskAttemptContext = mock(TaskAttemptContext.class);

    @Test
    public void testConstructor() {
        CombineFileInputFormat combineFileInputFormat = new OrcCombineFileInputFormat();
        assertTrue(combineFileInputFormat instanceof OrcCombineFileInputFormat);
    }

    @Test
    public void testCreateRecordReader() throws IOException {
        OrcCombineFileInputFormat combineFileInputFormat = new OrcCombineFileInputFormat();
        RecordReader recordReader =  combineFileInputFormat.createRecordReader(inputSplit, taskAttemptContext);
        assertTrue(recordReader instanceof CombineFileRecordReader);
    }

    @Test
    public void testIsSplitable() {
        OrcCombineFileInputFormat combineFileInputFormat = new OrcCombineFileInputFormat();
        assertFalse(combineFileInputFormat.isSplitable(context, new Path("/soruce/path")));
    }
}
