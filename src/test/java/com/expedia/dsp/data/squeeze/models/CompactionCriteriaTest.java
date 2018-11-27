package com.expedia.dsp.data.squeeze.models;

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
        final Map<String, String> options = retrieveOptions(null, "1000");
        CompactionCriteria criteria = new CompactionCriteria(options);
        assertNull(criteria.getThresholdInBytes());
    }

    @Test(expected = NumberFormatException.class)
    public void testWrongThreshold() throws Exception {
        final Map<String, String> options = retrieveOptions("1234a", "1000");
        CompactionCriteria criteria = new CompactionCriteria(options);
        assertNull(criteria.getThresholdInBytes());
    }

    @Test
    public void testNullMaxReducers() throws Exception {
        final Map<String, String> options = retrieveOptions("1234", null);
        CompactionCriteria criteria = new CompactionCriteria(options);
        assertNull(criteria.getMaxReducers());
    }

    @Test(expected = NumberFormatException.class)
    public void testWrongMaxReducers() throws Exception {
        final Map<String, String> options = retrieveOptions("1234", "1000a");
        CompactionCriteria criteria = new CompactionCriteria(options);
        assertNull(criteria.getMaxReducers());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoSchemaPath() throws Exception {
        final Map<String, String> options = retrieveOptions("1234", "1000");
        options.put("fileType", "AVRO");
        options.remove("schemaPath");
        CompactionCriteria criteria = new CompactionCriteria(options);
    }

    @Test
    public void testCriteria() throws Exception {
        Map<String, String> options = retrieveOptions("1234", "1000");

        CompactionCriteria criteria = new CompactionCriteria(options);
        assertEquals("sourcePath", criteria.getSourcePath());
        assertEquals("targetPath", criteria.getTargetPath());
        assertEquals(1234L, criteria.getThresholdInBytes(), 0);
        assertEquals("schemaPath", criteria.getSchemaPath());
        assertEquals(1000L, criteria.getMaxReducers(), 0);
        assertNull(criteria.getFileType());
        options.put("fileType", "AVRO");

        criteria = new CompactionCriteria(options);
        assertEquals(FileType.AVRO.getValue(), criteria.getFileType());

        criteria = new CompactionCriteria("source", "target", 12345L, 1000L);
        assertEquals("source", criteria.getSourcePath());
        assertEquals("target", criteria.getTargetPath());
        assertEquals(12345L, criteria.getThresholdInBytes(), 0);
        assertEquals(1000L, criteria.getMaxReducers(), 0);

        criteria = new CompactionCriteria("source", "target", 12345L, 1000L, "ORC", "schemaPath");
        assertEquals("source", criteria.getSourcePath());
        assertEquals("target", criteria.getTargetPath());
        assertEquals(12345L, criteria.getThresholdInBytes(), 0);
        assertEquals(1000L, criteria.getMaxReducers(), 0);
        assertEquals("ORC", criteria.getFileType());
        assertEquals("schemaPath", criteria.getSchemaPath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnsupportedFileFormat() {
        Map<String, String> options = retrieveOptions("1234", "1000");
        options.put("fileType", "AVROO");
        new CompactionCriteria(options);
    }

    private Map<String, String> retrieveOptions(final String threshold, final String maxReducers) {
        final Map<String, String> options = new HashMap<String, String>();
        options.put("sourcePath", "sourcePath");
        options.put("targetPath", "targetPath");
        options.put("thresholdInBytes", threshold);
        options.put("schemaPath", "schemaPath");
        options.put("maxReducers", maxReducers);
        return options;
    }
}
