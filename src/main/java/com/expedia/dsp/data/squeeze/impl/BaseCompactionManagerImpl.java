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
