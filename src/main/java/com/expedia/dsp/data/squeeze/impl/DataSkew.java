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
