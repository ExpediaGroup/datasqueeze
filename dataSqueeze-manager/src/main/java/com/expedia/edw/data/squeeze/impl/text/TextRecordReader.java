package com.expedia.edw.data.squeeze.impl.text;

import com.expedia.edw.data.squeeze.impl.CombineFileWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Text Record reader for combine file split.
 *
 * @author Yashraj R. Sontakke
 */
public class TextRecordReader extends RecordReader<CombineFileWritable, Text> {
    private long startOffset;
    private long end;
    private long pos;
    private FileSystem fs;
    private Path path;
    private Path dPath;
    private CombineFileWritable key = new CombineFileWritable();
    private Text value;
    private long rlength;
    private FSDataInputStream fileIn;
    private LineReader reader;

    public TextRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
        final Configuration currentConf = context.getConfiguration();
        this.path = split.getPath(index);
        boolean isCompressed = findCodec(currentConf);
        if (isCompressed)
            codecWiseDecompress(context.getConfiguration());

        fs = this.path.getFileSystem(currentConf);

        this.startOffset = split.getOffset(index);

        if (isCompressed) {
            this.end = startOffset + rlength;
        } else {
            this.end = startOffset + split.getLength(index);
            dPath = path;
        }
        boolean skipFirstLine = false;

        fileIn = fs.open(dPath);

        if (isCompressed) fs.deleteOnExit(dPath);

        if (startOffset != 0) {
            skipFirstLine = true;
            --startOffset;
            fileIn.seek(startOffset);
        }
        reader = new LineReader(fileIn);
        if (skipFirstLine) {
            startOffset += reader.readLine(new Text(), 0,
                    (int) Math.min((long) Integer.MAX_VALUE, end - startOffset));
        }
        this.pos = startOffset;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key.fileName == null) {
            key = new CombineFileWritable();
            key.fileName = dPath.toString();
        }
        key.offset = pos;
        if (value == null) {
            value = new Text();
        }
        int newSize = 0;
        if (pos < end) {
            newSize = reader.readLine(value);
            pos += newSize;
        }
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
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
        if (startOffset == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - startOffset) / (float)
                    (end - startOffset));
        }
    }

    @Override
    public void close() throws IOException {

    }

    private void codecWiseDecompress(Configuration conf) throws IOException {

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(path);

        if (codec == null) {
            System.err.println("No Codec Found For " + path);
            System.exit(1);
        }

        String outputUri =
                CompressionCodecFactory.removeSuffix(path.toString(),
                        codec.getDefaultExtension());
        dPath = new Path(outputUri);

        InputStream in = null;
        OutputStream out = null;
        fs = this.path.getFileSystem(conf);

        try {
            in = codec.createInputStream(fs.open(path));
            out = fs.create(dPath);
            IOUtils.copyBytes(in, out, conf);
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
            rlength = fs.getFileStatus(dPath).getLen();
        }
    }

    private boolean findCodec(Configuration conf) {

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(path);

        if (codec == null)
            return false;
        else
            return true;

    }
}
