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
package com.expedia.dsp.data.squeeze;

import com.expedia.dsp.data.squeeze.impl.DataSkew;

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
