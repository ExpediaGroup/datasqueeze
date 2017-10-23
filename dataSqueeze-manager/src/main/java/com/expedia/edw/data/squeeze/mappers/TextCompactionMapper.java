package com.expedia.edw.data.squeeze.mappers;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Mapper class for Text and Sequence input file formats.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class TextCompactionMapper extends Mapper<Object, Text, Text, Text> {

    final Map<String, Long> fileSizesMap = new HashMap<String, Long>();
    private FileSystem fileSystem;
    private Configuration configuration;
    long threshold;

    /**
     * {@inheritDoc}
     */
    protected void setup(Context context) throws IOException, InterruptedException {
        configuration = context.getConfiguration();
        final String compactionThreshold = configuration.get("compactionThreshold");
        threshold = Long.parseLong(compactionThreshold);
        log.info("Compaction threshold {}", threshold);
    }

    /**
     * {@inheritDoc}
     */
    protected void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        if (value!= null && value.toString() != null && value.toString().isEmpty()) {
            return;
        }

        // Mapper sends data with parent directory path as keys to retain directory structure
        final FileSplit fileSplit = (FileSplit) context.getInputSplit();
        final Path filePath = fileSplit.getPath();
        final String parentFilePath = String.format("%s/", filePath.getParent().toString());
        log.debug("Parent file path {}", parentFilePath);

        if (!fileSizesMap.containsKey(filePath.toString())) {
            if (fileSystem == null){
                final URI uri = URI.create(filePath.toString());
                fileSystem = FileSystem.get(uri, configuration);
            }
            final FileStatus[] listStatuses = fileSystem.listStatus(filePath);
            for (FileStatus fileStatus : listStatuses) {
                if (!fileStatus.isDirectory()) {
                    fileSizesMap.put(fileStatus.getPath().toString(), fileStatus.getLen());
                    log.info("Entry added to fileSizes Map {} {}", fileStatus.getPath().toString(), fileStatus.getLen());
                }
            }
        }

        final Text parentFilePathKey = new Text(parentFilePath);
        final Text filePathKey = new Text(filePath.toString());
        final Long fileSize = fileSizesMap.get(filePath.toString());
        if (fileSize < threshold) {
            context.write(parentFilePathKey, value);
        } else {
            context.write(filePathKey, value);
        }
    }
}
