package com.expedia.dsp.data.squeeze.models;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.Validate;

/**
 * Response for compaction.
 *
 * @author Yashraj R. Sontakke
 */
@Getter
@Setter
public class CompactionResponse {

    private String targetPath;
    private FileType targetFileType;
    private boolean isSuccessful;

    /**
     * Constructor
     *
     * @param isSuccessful   true when compaction was successful, false otherwise
     * @param targetPath     the target path for storing compacted files
     * @param targetFileType {@link FileType} target file type
     */
    public CompactionResponse(final boolean isSuccessful, final String targetPath, final FileType targetFileType) {
        Validate.notBlank(targetPath, "Target path cannot be null");
        Validate.notNull(targetFileType, "Target file type cannot be null");

        this.isSuccessful = isSuccessful;
        this.targetPath = targetPath;
        this.targetFileType = targetFileType;
    }
}
