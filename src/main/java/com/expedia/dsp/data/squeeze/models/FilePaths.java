/**
 * Copyright (C) 2018 Expedia Group
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
