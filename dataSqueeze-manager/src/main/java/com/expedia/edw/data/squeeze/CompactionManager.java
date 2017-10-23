package com.expedia.edw.data.squeeze;

import com.expedia.edw.data.squeeze.models.CompactionCriteria;
import com.expedia.edw.data.squeeze.models.CompactionResponse;

/**
 * Manager that performs compaction.
 *
 * @author Yashraj R. Sontakke
 */
public interface CompactionManager {

    /**
     * Perform compaction for files at source path and store compacted files at target path.
     * Retains the source directory structure at target directory.
     *
     * @return the {@link CompactionResponse}
     */
    CompactionResponse compact() throws Exception;

    /**
     * Validates {@link CompactionCriteria}.
     */
    void validate();
}
