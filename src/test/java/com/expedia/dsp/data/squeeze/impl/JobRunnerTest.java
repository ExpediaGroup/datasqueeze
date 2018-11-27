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
