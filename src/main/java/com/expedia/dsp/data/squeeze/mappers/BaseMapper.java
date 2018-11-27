package com.expedia.dsp.data.squeeze.mappers;

import com.expedia.dsp.data.squeeze.impl.DataSkew;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Base Mapper class for retrieving the mapper key.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class BaseMapper {

    private final Map<String, Long> fileSizesMap = new HashMap<String, Long>();
    private FileSystem fileSystem;
    private Configuration configuration;
    private long threshold;
    private JSONObject dataSkewObject;
    private DataSkew dataSkew;

    /**
     * Constructor
     *
     * @param context the {@link Context}
     */
    public BaseMapper(final Context context) {
        configuration = context.getConfiguration();
        final String compactionThreshold = configuration.get("compactionThreshold");
        threshold = Long.parseLong(compactionThreshold);
        log.info("Compaction threshold {}", threshold);
        final String compactionDataSkew = configuration.get("compactionDataSkew");
        if (!StringUtils.isBlank(compactionDataSkew)) {
            dataSkewObject = new JSONObject(compactionDataSkew);
            log.info("Compaction dataSkewObject {}", dataSkewObject.toString());
        }
        dataSkew = new DataSkew(dataSkewObject);
    }

    /**
     * Retrieves the mapper key.
     *
     * @param file the file
     * @return the mapper key as {@link Text}
     */
    protected Text getKey(final String file) throws IOException {
        // Mapper sends data with parent directory path as keys to retain directory structure

        final Path filePath = new Path(file);
        final String parentFilePath = String.format("%s/", filePath.getParent().toString());
        log.debug("Parent file path {}", parentFilePath);

        if (!fileSizesMap.containsKey(filePath.toString())) {
            if (fileSystem == null) {
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

        // File greater than threshold is not compacted
        if (fileSize > threshold) {
            return filePathKey;
        }

        // Handle data skew by writing data to multiple files rather than one
        if (dataSkewObject != null && dataSkewObject.has(parentFilePath)) {
            log.info("Processing file with dataSkew {}", parentFilePath);
            final Text skewKey = dataSkew.getSkewKey(parentFilePath, filePath.toString());
            return skewKey;
        }
        return parentFilePathKey;
    }
}
