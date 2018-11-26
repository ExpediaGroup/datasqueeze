package com.expedia.dsp.data.squeeze.impl.text;

import com.expedia.dsp.data.squeeze.impl.CombineFileWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * Text Combine file input format reads multiple files in same mapper.
 *
 * @author Yashraj R. Sontakke
 */
public class TextCombineFileInputFormat extends CombineFileInputFormat<CombineFileWritable, Text> {

    public TextCombineFileInputFormat() {
        super();
    }

    @Override
    public RecordReader<CombineFileWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<CombineFileWritable, Text>((CombineFileSplit) split, context, TextRecordReader.class);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}
