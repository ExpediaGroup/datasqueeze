package com.expedia.edw.data.squeeze.models;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Criteria for compaction.
 *
 * @author Yashraj R. Sontakke
 */
@Getter
@Slf4j
public class CompactionCriteria {

    /**
     * the source path for compaction
     */
    private String sourcePath;
    /**
     * the target path for compaction
     */
    private String targetPath;
    /**
     * threshold in bytes for compaction. If file size is greater then no compaction on file, file is just copied to
     * target directory else file is compacted. Optional parameter, if not provided, defaults to threshold from config
     * resources file (128 MB).
     */
    private Long thresholdInBytes;

    /**
     * Constructor
     *
     * @param options map of various options keyed by name
     */
    public CompactionCriteria(final Map<String, String> options) throws NumberFormatException {
        this.sourcePath = options.get("sourcePath");
        this.targetPath = options.get("targetPath");
        try {
            if (options.get("thresholdInBytes") != null) {
                this.thresholdInBytes = Long.parseLong(options.get("thresholdInBytes"));
            }
        } catch (NumberFormatException e) {
            log.error("Error converting threshold from string to long {}", e.getMessage());
            throw e;
        }
    }
    
    /**
     * Constructor
     *  @param sourcePath       the source path
     * @param targetPath       the target path
     * @param thresholdInBytes threshold in bytes for compaction. If file size is greater then no compaction on file,
 *                         file is just copied to target directory else file is compacted. Optional parameter, if
 *                         not provided, defaults to threshold from config resources file (128 MB).
     */
    public CompactionCriteria(final String sourcePath, final String targetPath, final Long thresholdInBytes) {
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.thresholdInBytes = thresholdInBytes;
    }
}
