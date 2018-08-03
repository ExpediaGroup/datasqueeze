package com.expedia.edw.data.squeeze.impl;

import com.expedia.edw.data.squeeze.CompactionManager;
import com.expedia.edw.data.squeeze.models.CompactionCriteria;
import com.expedia.edw.data.squeeze.models.CompactionResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.security.AccessControlException;

import java.net.URI;
import java.util.UUID;

/**
 * Inplace compaction implementation for {@link CompactionManager}.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class CompactionManagerInPlaceImpl extends BaseCompactionManagerImpl {

    /**
     * {@inheritDoc}
     */
    public CompactionManagerInPlaceImpl(final Configuration configuration, final CompactionCriteria criteria) {
        super(configuration, criteria);
    }

    /**
     * {@inheritDoc}
     */
    public void validate() {
        Validate.notBlank(criteria.getSourcePath(), "Source path cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    public CompactionResponse compact() throws Exception {
        Validate.notNull(criteria, "Criteria cannot be null");
        log.info("In place compaction requested for input path {}", criteria.getSourcePath());
        final URI uri = URI.create(criteria.getSourcePath());
        final FileSystem fileSystem = FileSystem.get(uri, configuration);

        final String tempCompactedLocation = TEMP_OUTPUT_LOCATION + "compacted-" + UUID.randomUUID().toString() + "/";
        final Path tempLocation = new Path(TEMP_OUTPUT_LOCATION + UUID.randomUUID().toString() + "/");
        try {
            fileSystem.access(new Path(criteria.getSourcePath()), FsAction.WRITE);
        } catch (AccessControlException e) {
            throw new IllegalStateException(String.format("User does not have permissions to perform move/delete for location %s", criteria.getSourcePath()));
        }
        // Perform normal compaction from Source --> TempTargetLocation
        log.info("Performing Normal Compaction from Source {} to Temp Target {}", criteria.getSourcePath(), tempCompactedLocation);
        final CompactionCriteria compactionCriteria = new CompactionCriteria(criteria.getSourcePath(), tempCompactedLocation, criteria.getThresholdInBytes(),
                criteria.getMaxReducers(), criteria.getFileType(), criteria.getSchemaPath());
        final CompactionManager compactionManager = new CompactionManagerImpl(configuration, compactionCriteria);
        final CompactionResponse response = compactionManager.compact();

        log.info("Moving files from input path {} to temp path {}", criteria.getSourcePath(), tempLocation.toString());
        fileSystem.rename(new Path(criteria.getSourcePath()), tempLocation);

        log.info("Moving compacted files from temp compacted path {} to final location {}", tempCompactedLocation, criteria.getSourcePath());
        fileSystem.rename(new Path(tempCompactedLocation), new Path(criteria.getSourcePath()));
        return response;
    }
}
