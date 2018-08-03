//package com.expedia.edw.data.squeeze.models;
//
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNull;
//
//import com.expedia.edw.data.squeeze.impl.CompactionManagerImpl;
//
///**
// * Tests for {@link CompactionCriteria}
// *
// * @author Yashraj R. Sontakke
// */
//public class CompactionCriteriaTest {
//
//    @Test
//    public void testNullThreshold() throws Exception {
//        final Map<String, String> options = retrieveOptions(null, "1000");
//        CompactionCriteria criteria = new CompactionCriteria(options);
//        assertNull(criteria.getThresholdInBytes());
//    }
//
//    @Test(expected = NumberFormatException.class)
//    public void testWrongThreshold() throws Exception {
//        final Map<String, String> options = retrieveOptions("1234a", "1000");
//        CompactionCriteria criteria = new CompactionCriteria(options);
//        assertNull(criteria.getThresholdInBytes());
//    }
//
//    @Test
//    public void testNullMaxReducers() throws Exception {
//        final Map<String, String> options = retrieveOptions("1234", null);
//        CompactionCriteria criteria = new CompactionCriteria(options);
//        assertNull(criteria.getMaxReducers());
//    }
//
//    @Test(expected = NumberFormatException.class)
//    public void testWrongMaxReducers() throws Exception {
//        final Map<String, String> options = retrieveOptions("1234", "1000a");
//        CompactionCriteria criteria = new CompactionCriteria(options);
//        assertNull(criteria.getMaxReducers());
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void testNoSchemaPath() throws Exception {
//        final Map<String, String> options = retrieveOptions("1234", "1000");
//        options.put("fileType", "AVRO");
//        CompactionCriteria criteria = new CompactionCriteria(options);
//    }
//
//    @Test
//    public void testCriteria() throws Exception {
//<<<<<<< HEAD
//        CompactionCriteria criteria = new CompactionCriteria("source", "target", 12345L, "temp", false, 1000L);
//        assertEquals("source", criteria.getSourcePath());
//        assertEquals("target", criteria.getTargetPath());
//        assertEquals(12345L, criteria.getThresholdInBytes(), 0);
//        assertEquals("temp", criteria.getTempPath());
//        assertFalse(criteria.isRetainTempFiles());
//        assertEquals(1000L, criteria.getMaxReducers(), 0);
//
//        criteria = new CompactionCriteria("source", "target", 12345L, "temp", true, 1000L);
//        assertTrue(criteria.isRetainTempFiles());
//
//        Map<String, String> options = retrieveOptions("1234", "1000");
//        options.put("retainTempFiles", "true");
//
//        criteria = new CompactionCriteria(options);
//        assertEquals("sourcePath", criteria.getSourcePath());
//        assertEquals("targetPath", criteria.getTargetPath());
//        assertEquals(1234L, criteria.getThresholdInBytes(), 0);
//        assertEquals("tempPath", criteria.getTempPath());
//        assertTrue(criteria.isRetainTempFiles());
//        assertEquals(1000L, criteria.getMaxReducers(), 0);
//
//        options.put("retainTempFiles", "false");
//        criteria = new CompactionCriteria(options);
//        assertFalse(criteria.isRetainTempFiles());
//
//        options.put("retainTempFiles", "abcd");
//        criteria = new CompactionCriteria(options);
//        assertFalse(criteria.isRetainTempFiles());
//
//        assertNull(criteria.getFileType());
//        assertNull(criteria.getSchemaPath());
//
//        options.put("fileType", "AVRO");
//        options.put("schemaPath", "schemaPath");
//        criteria = new CompactionCriteria(options);
//
//        assertEquals(FileType.AVRO.getValue(), criteria.getFileType());
//        assertEquals("schemaPath", criteria.getSchemaPath());
//
//        options.put("fileType", "ORC");
//        options.remove("schemaPath");
//        criteria = new CompactionCriteria(options);
//
//        assertEquals(FileType.ORC.getValue(), criteria.getFileType());
//
//=======
//        CompactionCriteria criteria = new CompactionCriteria("source", "target", 12345L);
//        assertEquals("source", criteria.getSourcePath());
//        assertEquals("target", criteria.getTargetPath());
//        assertEquals(12345L, criteria.getThresholdInBytes(), 0);
//>>>>>>> develop
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void testUnsupportedFileFormat() {
//        Map<String, String> options = retrieveOptions("1234", "1000");
//        options.put("fileType", "AVROO");
//        new CompactionCriteria(options);
//    }
//
//    private Map<String, String> retrieveOptions(final String threshold, final String maxReducers) {
//        final Map<String, String> options = new HashMap<String, String>();
//        options.put("sourcePath", "sourcePath");
//        options.put("targetPath", "targetPath");
//        options.put("thresholdInBytes", threshold);
//        options.put("tempPath", "tempPath");
//        options.put("maxReducers", maxReducers);
//        return options;
//    }
//
//}
