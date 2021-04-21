/**
 * Copyright (C) 2017-2021 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
