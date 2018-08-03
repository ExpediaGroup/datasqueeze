package com.expedia.edw.data.squeeze.impl;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.expedia.edw.data.squeeze.SchemaSelector;
import com.expedia.edw.data.squeeze.models.CompactionCriteria;
import com.expedia.edw.data.squeeze.models.FileType;

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
