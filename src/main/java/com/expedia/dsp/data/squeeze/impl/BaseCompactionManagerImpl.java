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

import com.expedia.dsp.data.squeeze.CompactionManager;
import com.expedia.dsp.data.squeeze.models.CompactionCriteria;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;

/**
 * Base Implementation for {@link CompactionManager}.
 *
 * @author Yashraj R. Sontakke
 */
public abstract class BaseCompactionManagerImpl implements CompactionManager {

    protected final Configuration configuration;
    protected final CompactionCriteria criteria;
    protected static final String TEMP_OUTPUT_LOCATION = "/apps/datasqueeze/staging/tmp-";

    /**
     * Constructor
     *
     * @param configuration the {@link Configuration}.
     * @param criteria      the {@link CompactionCriteria}.
     */
    public BaseCompactionManagerImpl(final Configuration configuration, final CompactionCriteria criteria) {
        Validate.notNull(configuration, "Configuration cannot be null");
        Validate.notNull(criteria, "Criteria cannot be null");
        this.configuration = configuration;
        this.criteria = criteria;
        validate();
    }
}
