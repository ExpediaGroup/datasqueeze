package com.expedia.edw.data.squeeze.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

/**
 * Job Runner to run the job.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class JobRunner extends Configured implements Tool {

    private final Job job;

    /**
     * Constructor
     *
     * @param job the {@link Job}
     */
    public JobRunner(final Job job) {
        this.job = job;
    }

    /**
     * {@inheritDoc}
     */
    public int run(final String[] args) throws Exception {
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
