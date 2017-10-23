package com.expedia.edw.data.squeeze;

import com.expedia.edw.data.squeeze.impl.CompactionManagerImpl;
import com.expedia.edw.data.squeeze.impl.CompactionManagerInPlaceImpl;
import com.expedia.edw.data.squeeze.models.CompactionCriteria;
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

    /**
     * Retrieves {@link CompactionManager}.
     *
     * @param options map of various options keyed by name
     * @return {@link CompactionManager}
     */
    public static CompactionManager create(Map<String, String> options) throws Exception {
        final Configuration configuration = new Configuration();
        configuration.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        configuration.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        configuration.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
        configuration.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));

        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONF_PATH);
            DEFAULT_THRESHOLD_IN_BYTES = config.getLong("default.threshold");
        } catch (Exception e) {
            throw new RuntimeException("Exception while loading default threshold in bytes" + e);

        }
        final CompactionCriteria criteria = new CompactionCriteria(options);
        if (StringUtils.isNotBlank(options.get("targetPath"))) {
            return new CompactionManagerImpl(configuration, criteria);
        }
        return new CompactionManagerInPlaceImpl(configuration, criteria);
    }
}
