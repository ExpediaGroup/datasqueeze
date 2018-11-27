package com.expedia.dsp.data.squeeze.impl.orc;

import com.expedia.dsp.data.squeeze.impl.CombineFileWritable;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcMapredRecordReader;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.util.List;

/**
 * ORC Record reader for combine file split.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class OrcRecordReader extends RecordReader<CombineFileWritable, OrcStruct> {
    private org.apache.orc.RecordReader in;
    private long offset;
    private long end;
    protected Configuration conf;
    private Path path;
    private CombineFileWritable key = new CombineFileWritable();
    private final TypeDescription schema;
    private final VectorizedRowBatch batch;
    private int rowInBatch;
    private final OrcStruct row;

    public OrcRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {

        this.path = split.getPath(index);
        this.offset = split.getOffset(index);
        this.end = offset + split.getLength(index);

        final Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(context.getConfiguration()));
        final Reader.Options options = new Reader.Options();
        options.range(offset, split.getLength(index));

        in = reader.rows(options);
        schema = reader.getSchema();
        this.batch = schema.createRowBatch();
        rowInBatch = 0;
        this.row = (OrcStruct) OrcStruct.createValue(schema);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

    }

    /**
     * If the current batch is empty, get a new one.
     *
     * @return true if we have rows available.
     * @throws IOException
     */
    boolean ensureBatch() throws IOException {
        if (rowInBatch >= batch.size) {
            rowInBatch = 0;
            return in.nextBatch(batch);
        }
        return true;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null || key.fileName == null) {
            key = new CombineFileWritable();
            key.fileName = path.toString();
        }
        if (!ensureBatch()) {
            return false;
        }
        if (schema.getCategory() == TypeDescription.Category.STRUCT) {
            OrcStruct result = row;
            List<TypeDescription> children = schema.getChildren();
            int numberOfChildren = children.size();
            for (int i = 0; i < numberOfChildren; ++i) {
                result.setFieldValue(i, OrcMapredRecordReader.nextValue(batch.cols[i], rowInBatch,
                        children.get(i), result.getFieldValue(i)));
            }
        } else {
            OrcMapredRecordReader.nextValue(batch.cols[0], rowInBatch, schema, row);
        }
        rowInBatch += 1;
        return true;
    }

    @Override
    public CombineFileWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public OrcStruct getCurrentValue() throws IOException, InterruptedException {
        return row;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return in.getProgress();
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
