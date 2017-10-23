package com.expedia.edw.data.squeeze.models;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for {@link CompactionCriteria}
 *
 * @author Yashraj R. Sontakke
 */
public class CompactionCriteriaTest {

    @Test
    public void testNullThreshold() throws Exception {
        final Map<String, String> options = retrieveOptions(null);
        CompactionCriteria criteria = new CompactionCriteria(options);
        assertNull(criteria.getThresholdInBytes());
    }

    @Test(expected = NumberFormatException.class)
    public void testWrongThreshold() throws Exception {
        final Map<String, String> options = retrieveOptions("1234a");
        CompactionCriteria criteria = new CompactionCriteria(options);
        assertNull(criteria.getThresholdInBytes());
    }

    @Test
    public void testCriteria() throws Exception {
        CompactionCriteria criteria = new CompactionCriteria("source", "target", 12345L);
        assertEquals("source", criteria.getSourcePath());
        assertEquals("target", criteria.getTargetPath());
        assertEquals(12345L, criteria.getThresholdInBytes(), 0);
    }

    private Map<String, String> retrieveOptions(final String threshold) {
        final Map<String, String> options = new HashMap<String, String>();
        options.put("sourcePath", "sourcePath");
        options.put("targetPath", "targetPath");
        options.put("thresholdInBytes", threshold);
        options.put("tempPath", "tempPath");
        return options;
    }
}
