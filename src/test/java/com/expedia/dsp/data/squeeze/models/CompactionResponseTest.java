package com.expedia.dsp.data.squeeze.models;

import org.junit.Test;

import static com.expedia.dsp.data.squeeze.models.FileType.*;
import static org.junit.Assert.*;

/**
 * Tests for {@link CompactionResponse}
 *
 * @author Yashraj R. Sontakke
 */
public class CompactionResponseTest {

    @Test
    public void testTargetPath() {
        try {
            new CompactionResponse(true, null, ORC);
            fail();
        } catch (NullPointerException e) {

        }

        try {
            new CompactionResponse(true, "", ORC);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new CompactionResponse(true, "  ", ORC);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }

    @Test(expected = NullPointerException.class)
    public void testSourceFilePath() {
        new CompactionResponse(true, "target", null);
    }


    @Test
    public void testCriteria() {
        CompactionResponse response = new CompactionResponse(true, "target", SEQ);
        assertTrue(response.isSuccessful());
        assertEquals("target", response.getTargetPath());
        assertEquals(SEQ, response.getTargetFileType());

        response.setTargetFileType(ORC);
        assertEquals(ORC, response.getTargetFileType());

        response.setTargetFileType(TEXT);
        assertEquals(TEXT, response.getTargetFileType());

        response.setSuccessful(false);
        assertFalse(response.isSuccessful());
    }
}
