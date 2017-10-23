package com.expedia.edw.data.squeeze;

import com.expedia.edw.data.squeeze.impl.CompactionManagerImpl;
import com.expedia.edw.data.squeeze.impl.CompactionManagerInPlaceImpl;
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
        assertEquals(134217728L, CompactionManagerFactory.DEFAULT_THRESHOLD_IN_BYTES, 0);
    }

    @Test
    public void testCreateCompactionManagerInPlaceImplNullTarget() throws Exception {
        final Map<String, String> options = retrieveOptions(null);
        CompactionManager compactionManager = CompactionManagerFactory.create(options);
        assertNotNull(compactionManager);
        assertTrue(compactionManager instanceof CompactionManagerInPlaceImpl);
        assertEquals(134217728L, CompactionManagerFactory.DEFAULT_THRESHOLD_IN_BYTES, 0);
    }

    @Test
    public void testCreateCompactionManagerInPlaceImplEmptyTarget() throws Exception {
        final Map<String, String> options = retrieveOptions("");
        CompactionManager compactionManager = CompactionManagerFactory.create(options);
        assertNotNull(compactionManager);
        assertTrue(compactionManager instanceof CompactionManagerInPlaceImpl);
        assertEquals(134217728L, CompactionManagerFactory.DEFAULT_THRESHOLD_IN_BYTES, 0);
    }

    @Test
    public void testCreateCompactionManagerInPlaceImplBlankTarget() throws Exception {
        final Map<String, String> options = retrieveOptions("  ");
        CompactionManager compactionManager = CompactionManagerFactory.create(options);
        assertNotNull(compactionManager);
        assertTrue(compactionManager instanceof CompactionManagerInPlaceImpl);
        assertEquals(134217728L, CompactionManagerFactory.DEFAULT_THRESHOLD_IN_BYTES, 0);
    }

    @Test
    public void testCreateCompactionManagerInPlaceImplNoTarget() throws Exception {
        final Map<String, String> options = retrieveOptions("");
        options.remove("targetPath");
        CompactionManager compactionManager = CompactionManagerFactory.create(options);
        assertNotNull(compactionManager);
        assertTrue(compactionManager instanceof CompactionManagerInPlaceImpl);
        assertEquals(134217728L, CompactionManagerFactory.DEFAULT_THRESHOLD_IN_BYTES, 0);
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
