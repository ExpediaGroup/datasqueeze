package com.expedia.edw.data.squeeze;

import com.expedia.edw.data.squeeze.impl.DataSkew;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for {@link DataSkew}
 *
 * @author Yashraj R. Sontakke
 */
public class DataSkewTest {

    @Test
    public void testGetSkewKey() {
        JSONObject dataSkewObject = new JSONObject();
        dataSkewObject.put("/source/A/", "1");
        dataSkewObject.put("/source/B/", "2");

        final DataSkew dataSkew = new DataSkew(dataSkewObject);
        final Text key1 = dataSkew.getSkewKey("/source/A/", "/source/A/file1");
        assertNotNull(key1);

        final Text key2 = dataSkew.getSkewKey("/source/A/", "/source/A/file2");
        assertEquals(key1, key2);

        final Text key3 = dataSkew.getSkewKey("/source/B/", "/source/B/file1");
        assertNotEquals(key1, key3);
    }
}
