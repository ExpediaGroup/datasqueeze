package com.expedia.edw.data.squeeze.models;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * File Paths object to store bytes and paths.
 *
 * @author Yashraj R. Sontakke
 */
@Getter
@Slf4j
public class FilePaths {
    private Long bytes = 0L;
    private Map<Path, Long> pathsByBytes = new HashMap<Path, Long>();

    /**
     * Adds a file to the object
     *
     * @param file {@link LocatedFileStatus}
     */
    public void addFile(final LocatedFileStatus file) {
        bytes += file.getLen();
        pathsByBytes.put(file.getPath(), file.getLen());
    }

    /**
     * Retrieves list of all {@link Path}
     *
     * @return list of all {@link Path}
     */
    public List<Path> getAllFilePaths() {
        final List<Path> paths = new ArrayList<Path>();
        paths.addAll(pathsByBytes.keySet());
        return paths;
    }
}
