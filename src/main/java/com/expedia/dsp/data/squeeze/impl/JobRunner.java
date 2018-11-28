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
