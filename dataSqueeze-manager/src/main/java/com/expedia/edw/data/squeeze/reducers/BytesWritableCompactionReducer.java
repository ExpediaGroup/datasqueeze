package com.expedia.edw.data.squeeze.reducers;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.net.URI;

/**
 * Reducer class for Sequence input file format with BytesWritable value format.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class BytesWritableCompactionReducer extends Reducer<Text, BytesWritable, NullWritable, BytesWritable> {

    /**
     * {@inheritDoc}
     */
    protected void reduce(final Text key, final Iterable<BytesWritable> values, final Context context) throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        final String sourcePath = configuration.get("compactionSourcePath");
        final String targetPath = configuration.get("compactionTargetPath");

        // Reducer stores data at the target directory retaining the directory structure of files
        String filePath = key.toString().replace(sourcePath, targetPath);
        if (key.toString().endsWith("/")) {
            filePath = filePath.concat("file");
        }

        log.info("Compaction output path {}", filePath);
        final URI uri = URI.create(filePath);
        final MultipleOutputs multipleOutputs = new MultipleOutputs<NullWritable, BytesWritable>(context);
        try {
            for (final BytesWritable text : values) {
                multipleOutputs.write(NullWritable.get(), text, uri.toString());
            }
        } finally {
            multipleOutputs.close();
        }
    }
}
