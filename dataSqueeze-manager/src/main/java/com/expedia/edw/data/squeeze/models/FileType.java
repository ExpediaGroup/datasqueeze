package com.expedia.edw.data.squeeze.models;

import lombok.Getter;

/**
 * File types supported for compaction.
 *
 * @author Yashraj R. Sontakke
 */
@Getter
public enum FileType {
    TEXT("text/plain"), ORC("ORC"), SEQ("SEQ"), AVRO("AVRO");

    private final String value;

    private FileType(final String value) {
        this.value = value;
    }

}
