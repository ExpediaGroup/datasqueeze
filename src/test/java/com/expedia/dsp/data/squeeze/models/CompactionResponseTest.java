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
