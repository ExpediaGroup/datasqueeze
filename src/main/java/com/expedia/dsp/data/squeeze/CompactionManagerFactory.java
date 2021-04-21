/**
 * Copyright (C) 2017-2021 Expedia, Inc.
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
package com.expedia.dsp.data.squeeze;

import com.expedia.dsp.data.squeeze.impl.CompactionManagerImpl;
import com.expedia.dsp.data.squeeze.impl.CompactionManagerInPlaceImpl;
import com.expedia.dsp.data.squeeze.models.CompactionCriteria;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Map;

/**
 * Factory to retrieve {@link CompactionManager}
 *
 * @author Yashraj R. Sontakke
 */
public class CompactionManagerFactory {

    private static final String CONF_PATH = "compaction.properties";
    public static Long DEFAULT_THRESHOLD_IN_BYTES;
    public static Long MAX_REDUCERS;
    public static Long BYTES_PER_REDUCER;
    public static Double DATA_SKEW_FACTOR;
    public static String HADOOP_CONF;

    /**
     * Retrieves {@link CompactionManager}.
     *
     * @param options map of various options keyed by name
     * @return {@link CompactionManager}
     */
    public static CompactionManager create(Map<String, String> options) throws Exception {
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONF_PATH);
            DEFAULT_THRESHOLD_IN_BYTES = config.getLong("default.threshold");
            MAX_REDUCERS = config.getLong("max.reducers");
            BYTES_PER_REDUCER = config.getLong("bytes.per.reducer");
            DATA_SKEW_FACTOR = config.getDouble("data.skew.factor");
            HADOOP_CONF = config.getString("hadoop.conf");
        } catch (Exception e) {
            throw new RuntimeException("Exception while loading default properties" + e);

        }
        final Configuration configuration = new Configuration();
        configuration.addResource(new Path(String.format("%shdfs-site.xml", HADOOP_CONF)));
        configuration.addResource(new Path(String.format("%score-site.xml", HADOOP_CONF)));
        configuration.addResource(new Path(String.format("%syarn-site.xml", HADOOP_CONF)));
        configuration.addResource(new Path(String.format("%smapred-site.xml", HADOOP_CONF)));

        final CompactionCriteria criteria = new CompactionCriteria(options);
        if (StringUtils.isNotBlank(options.get("targetPath"))) {
            return new CompactionManagerImpl(configuration, criteria);
        }
        return new CompactionManagerInPlaceImpl(configuration, criteria);
    }
}
