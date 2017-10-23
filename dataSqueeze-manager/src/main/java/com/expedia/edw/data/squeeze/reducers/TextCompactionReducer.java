package com.expedia.edw.data.squeeze.reducers;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.net.URI;

/**
 * Reducer class for Text and Sequence input file formats.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class TextCompactionReducer extends Reducer<Text, Text, NullWritable, Text> {

    /**
     * {@inheritDoc}
     */
    protected void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
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
        final MultipleOutputs multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        try {
            for (final Text text : values) {
                multipleOutputs.write(NullWritable.get(), text, uri.toString());
            }
        } finally {
            multipleOutputs.close();
        }
    }
}
