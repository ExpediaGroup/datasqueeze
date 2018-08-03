package com.expedia.edw.data.squeeze.impl.orc;

import com.expedia.edw.data.squeeze.impl.CombineFileWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;

/**
 * ORC Combine file input format reads multiple files in same mapper.
 *
 * @author Yashraj R. Sontakke
 */
public class OrcCombineFileInputFormat extends CombineFileInputFormat<CombineFileWritable, OrcStruct> {

    public OrcCombineFileInputFormat() {
        super();
    }

    @Override
    public RecordReader<CombineFileWritable, OrcStruct> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<CombineFileWritable, OrcStruct>((CombineFileSplit) split, context, OrcRecordReader.class);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}
