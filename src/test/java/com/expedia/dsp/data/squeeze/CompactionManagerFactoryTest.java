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

import com.expedia.dsp.data.squeeze.impl.CompactionManagerImpl;
import com.expedia.dsp.data.squeeze.impl.CompactionManagerInPlaceImpl;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Tests for {@link CompactionManagerFactory}
 *
 * @author Yashraj R. Sontakke
 */
public class CompactionManagerFactoryTest {

    @Test
    public void testCreateCompactionManagerImpl() throws Exception {
        final Map<String, String> options = retrieveOptions("targetPath");
        CompactionManager compactionManager = CompactionManagerFactory.create(options);
        assertNotNull(compactionManager);
        assertTrue(compactionManager instanceof CompactionManagerImpl);
        assertConfigProperties();
    }

    @Test
    public void testCreateCompactionManagerInPlaceImplNullTarget() throws Exception {
        final Map<String, String> options = retrieveOptions(null);
        CompactionManager compactionManager = CompactionManagerFactory.create(options);
        assertNotNull(compactionManager);
        assertTrue(compactionManager instanceof CompactionManagerInPlaceImpl);
        assertConfigProperties();
    }

    @Test
    public void testCreateCompactionManagerInPlaceImplEmptyTarget() throws Exception {
        final Map<String, String> options = retrieveOptions("");
        CompactionManager compactionManager = CompactionManagerFactory.create(options);
        assertNotNull(compactionManager);
        assertTrue(compactionManager instanceof CompactionManagerInPlaceImpl);
        assertConfigProperties();
    }

    @Test
    public void testCreateCompactionManagerInPlaceImplBlankTarget() throws Exception {
        final Map<String, String> options = retrieveOptions("  ");
        CompactionManager compactionManager = CompactionManagerFactory.create(options);
        assertNotNull(compactionManager);
        assertTrue(compactionManager instanceof CompactionManagerInPlaceImpl);
        assertConfigProperties();
    }

    @Test
    public void testCreateCompactionManagerInPlaceImplNoTarget() throws Exception {
        final Map<String, String> options = retrieveOptions("");
        options.remove("targetPath");
        CompactionManager compactionManager = CompactionManagerFactory.create(options);
        assertNotNull(compactionManager);
        assertTrue(compactionManager instanceof CompactionManagerInPlaceImpl);
        assertConfigProperties();
    }

    private void assertConfigProperties() {
        assertEquals(134217728L, CompactionManagerFactory.DEFAULT_THRESHOLD_IN_BYTES, 0);
        assertEquals(2000L, CompactionManagerFactory.MAX_REDUCERS, 0);
        assertEquals(1073741824L, CompactionManagerFactory.BYTES_PER_REDUCER, 0);
        assertEquals("/etc/hadoop/conf/", CompactionManagerFactory.HADOOP_CONF);
    }

    private Map<String, String> retrieveOptions(final String targetPath) {
        final Map<String, String> options = new HashMap<String, String>();
        options.put("sourcePath", "sourcePath");
        options.put("targetPath", targetPath);
        options.put("thresholdInBytes", "1234");
        options.put("tempPath", "tempPath");
        return options;
    }
}
