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
package com.expedia.dsp.data.squeeze.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Provides the file key for the data skewed folder.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class DataSkew {

    private Random random = new Random();
    private final JSONObject dataSkewObject;
    private final Map<String, String> fileSkewsMap = new HashMap<String, String>();

    public DataSkew(final JSONObject dataSkewObject) {
        this.dataSkewObject = dataSkewObject;
    }

    /**
     * Retrieves the data skew solution key.
     *
     * @param parentFilePath the parent file path.
     * @return the data skew solution key.
     */
    public Text getSkewKey(final String parentFilePath, final String filePath) {

        if (fileSkewsMap.containsKey(filePath)) {
            return new Text(fileSkewsMap.get(filePath));
        }

        final Integer skew = dataSkewObject.getInt(parentFilePath);
        int randomSkewRandom = random.nextInt(skew) + 1;

        final String skewPath = String.format("%scompactedFile-%s", parentFilePath, randomSkewRandom);
        log.info("Skew File Path {}", skewPath);
        fileSkewsMap.put(filePath, skewPath);
        return new Text(skewPath);
    }
}
