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
package com.expedia.dsp.data.squeeze.impl;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.expedia.dsp.data.squeeze.SchemaSelector;
import com.expedia.dsp.data.squeeze.models.CompactionCriteria;
import com.expedia.dsp.data.squeeze.models.FileType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchemaSelectorImpl implements SchemaSelector {

    private CompactionCriteria criteria;
    private FileSystem fileSystem;

    public SchemaSelectorImpl(CompactionCriteria criteria, FileSystem fileSystem) {
        this.criteria = criteria;
        this.fileSystem = fileSystem;
    }

    @Override
    public String getSchemaJSON() {

        switch (FileType.valueOf(criteria.getFileType())) {
            case AVRO: {
                return getAvroSchemaJSON(criteria, fileSystem);
            }
            default:
                return null;
        }
    }

    private String getAvroSchemaJSON(final CompactionCriteria criteria, final FileSystem fileSystem) {
        try {
            log.info("Schema path is {}", criteria.getSchemaPath());
            Path path = new Path(criteria.getSchemaPath());
            FSDataInputStream fsDataInputStream = fileSystem.open(path);
            Schema schema = new Schema.Parser().parse(fsDataInputStream);
            return schema.toString();
        } catch (IOException e) {
            log.error("ERROR: Schema is not present at given path or schema is not parsable: {}", criteria.getSchemaPath());
            throw new IllegalArgumentException();
        }
    }
}
