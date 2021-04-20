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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.EnumUtils;

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
     * Max reducers for the map reduce job.
     */
    private Long maxReducers;

    /**
     * File type to initiate valid Map Reduce job.
     * Its hard to determine the file types for few formats.
     */
    private String fileType;

    /**
     * The schema path which should be used for compaction
     */
    private String schemaPath;


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

        try {
            if (options.get("maxReducers") != null) {
                this.maxReducers = Long.parseLong(options.get("maxReducers"));
            }
        } catch (NumberFormatException e) {
            log.error("Error converting maxReducers from string to long {}", e.getMessage());
            throw e;
        }
        if (options.get("fileType") != null) {

            if (EnumUtils.isValidEnum(FileType.class, options.get("fileType"))) {
                this.fileType = options.get("fileType");
            } else {
                log.error("ERROR: Unsupported file type {}, we currently support only TEXT, SEQ, ORC and AVRO", options.get("fileType"));
                throw new IllegalArgumentException();
            }
        }
        if (options.get("schemaPath") != null) {
            this.schemaPath = options.get("schemaPath");
        }

        if (this.fileType != null && this.fileType.equals("AVRO") && this.schemaPath == null) {
            log.error("ERROR: For file type AVRO schema path is mandatory.");
            throw new IllegalArgumentException();
        }

    }

    /**
     * Constructor
     *
     * @param sourcePath       the source path
     * @param targetPath       the target path
     * @param thresholdInBytes threshold in bytes for compaction. If file size is greater then no compaction on file,
     *                         file is just copied to target directory else file is compacted. Optional parameter, if
     *                         not provided, defaults to threshold from config resources file (128 MB).
     * @param maxReducers      max reducers for the map reduce job
     */
    public CompactionCriteria(final String sourcePath, final String targetPath, final Long thresholdInBytes,
                              final Long maxReducers) {
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.thresholdInBytes = thresholdInBytes;
        this.maxReducers = maxReducers;
    }

    /**
     * Constructor
     *
     * @param sourcePath       the source path
     * @param targetPath       the target path
     * @param thresholdInBytes threshold in bytes for compaction. If file size is greater then no compaction on file,
     *                         file is just copied to target directory else file is compacted. Optional parameter, if
     *                         not provided, defaults to threshold from config resources file (128 MB).
     * @param maxReducers      max reducers for the map reduce job
     * @param fileType         type of the file we are compacting (mandatory for AVRO)
     * @param schemaPath       Path to schema when file type is AVRO.
     */
    public CompactionCriteria(final String sourcePath, final String targetPath, final Long thresholdInBytes,
                              final Long maxReducers, final String fileType, final String schemaPath) {
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.thresholdInBytes = thresholdInBytes;
        this.maxReducers = maxReducers;
        this.fileType = fileType;
        this.schemaPath = schemaPath;
    }
}
