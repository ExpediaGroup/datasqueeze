package com.expedia.dsp.data.squeeze.impl.seq;

import com.expedia.dsp.data.squeeze.impl.CombineFileWritable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * Sequence Record reader for combine file split.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class SeqRecordReader extends RecordReader<CombineFileWritable, Text> {
    private SequenceFile.Reader in;
    private long offset;
    private long end;
    private boolean more = true;
    private Text value = null;
    protected Configuration conf;
    private Path path;
    private long pos;
    private long start;
    private CombineFileWritable key = new CombineFileWritable();
    private String keyClassString;
    private Class keyClass;

    public SeqRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {

        this.path = split.getPath(index);
        this.offset = split.getOffset(index);
        this.end = offset + split.getLength(index);

        final SequenceFile.Reader.Option startOption = SequenceFile.Reader.start(offset);
        final SequenceFile.Reader.Option lengthOption = SequenceFile.Reader.length(split.getLength(index));
        final SequenceFile.Reader.Option pathOption = SequenceFile.Reader.file(path);

        conf = context.getConfiguration();
        in = new SequenceFile.Reader(conf, startOption, lengthOption, pathOption);

        if (offset > in.getPosition()) {
            in.sync(offset);                  // sync to start
        }
        this.start = in.getPosition();
        pos = in.getPosition();

        more = start < end;
        keyClassString = in.getKeyClassName();
        keyClass = WritableName.getClass(in.getKeyClassName(), conf);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!more) {
            return false;
        }
        pos = in.getPosition();
        if (key.fileName == null) {
            key = new CombineFileWritable();
            key.fileName = path.toString();
        }
        key.offset = pos;
        WritableComparable key_1 = null;
        if ("org.apache.hadoop.io.NullWritable".equalsIgnoreCase(keyClassString)) {
            key_1 = NullWritable.get();
        } else {
            try {
                key_1 = (WritableComparable) keyClass.newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
                throw new IOException(String.format("Failed creating class %s", keyClassString));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
                throw new IOException(String.format("Failed creating class %s", keyClassString));
            }
        }
        more = in.next(key_1);

        if (value == null) {
            value = new Text();
        }

        if (!more && pos >= end) {
            if (!in.syncSeen()) {
                in.sync(in.getPosition());
            }
            more = false;
            key = null;
            value = null;
        } else {
            in.getCurrentValue(value);
        }
        return more;
    }

    @Override
    public CombineFileWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (in.getPosition() - start) / (float)
                    (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
