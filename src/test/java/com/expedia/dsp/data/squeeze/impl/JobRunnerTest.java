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
package com.expedia.dsp.data.squeeze.impl;

import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for {@link JobRunner}
 *
 * @author Yashraj R. Sontakke
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({Job.class, JobRunner.class})
public class JobRunnerTest {

    private final Job job = mock(Job.class);
    private final String[] args = new String[2];
    private final JobRunner jobRunner = new JobRunner(job);

    @Test
    public void testRunSuccessful() throws Exception {
        when(job.waitForCompletion(true)).thenReturn(true);
        assertEquals(0, jobRunner.run(args));
    }

    @Test
    public void testRunUnsuccessful() throws Exception {
        assertEquals(1, jobRunner.run(args));
    }
}
