package com.expedia.edw.data.squeeze.impl;

import com.expedia.edw.data.squeeze.CompactionManagerFactory;
import com.expedia.edw.data.squeeze.models.CompactionCriteria;
import com.expedia.edw.data.squeeze.models.CompactionResponse;
import com.expedia.edw.data.squeeze.models.FileType;
import net.sf.jmimemagic.Magic;
import net.sf.jmimemagic.MagicMatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.mapreduce.Job;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URI;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * Tests for {@link CompactionManagerImpl}
 *
 * @author Yashraj R. Sontakke
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CompactionManagerImpl.class, CompactionManagerInPlaceImpl.class, FileSystem.class, FSDataInputStream.class, OrcFile.class, Reader.class, Job.class
        , Magic.class, MagicMatch.class, UUID.class})
public class CompactionManagerInPlaceImplTest {

    private final Configuration configuration = mock(Configuration.class);
    private final FileSystem fileSystem = mock(FileSystem.class);
    private final CompactionManagerImpl compactionManagerImpl = PowerMockito.mock(CompactionManagerImpl.class);

    @Test(expected = NullPointerException.class)
    public void testNullConfiguration() {
        final CompactionCriteria criteria = new CompactionCriteria(null, "target/path", null);
        new CompactionManagerInPlaceImpl(null, criteria);
    }

    @Test(expected = NullPointerException.class)
    public void testNullCriteria() throws Exception {
        new CompactionManagerInPlaceImpl(configuration, null);
    }

    @Test(expected = NullPointerException.class)
    public void testNullSourcePath() {
        final CompactionCriteria criteria = new CompactionCriteria(null, null, null);
        new CompactionManagerImpl(null, criteria);
    }

    @Before
    public void setup() throws Exception {

        PowerMockito.mockStatic(FileSystem.class);
        when(FileSystem.get(configuration)).thenReturn(fileSystem);
        when(FileSystem.get(any(URI.class), any(Configuration.class))).thenReturn(fileSystem);
        CompactionManagerFactory.DEFAULT_THRESHOLD_IN_BYTES = 1234L;
    }

    @Test
    public void testCompactOnPrem() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria("/source/path", null, null);
        final CompactionResponse compactionResponse = new CompactionResponse(true, "/source/path", FileType.TEXT);
        whenNew(CompactionManagerImpl.class).withArguments(any(Configuration.class), any(CompactionCriteria.class)).thenReturn(compactionManagerImpl);
        when(compactionManagerImpl.compact()).thenReturn(compactionResponse);
        final CompactionManagerInPlaceImpl compactionManager = new CompactionManagerInPlaceImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(true, FileType.TEXT, criteria.getSourcePath(), response);
    }

    @Test
    public void testCompactOnS3() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria("s3/source/path", null, null);
        final CompactionResponse compactionResponse = new CompactionResponse(true, "s3/source/path", FileType.TEXT);
        whenNew(CompactionManagerImpl.class).withArguments(any(Configuration.class), any(CompactionCriteria.class)).thenReturn(compactionManagerImpl);
        when(compactionManagerImpl.compact()).thenReturn(compactionResponse);
        final CompactionManagerInPlaceImpl compactionManager = new CompactionManagerInPlaceImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(true, FileType.TEXT, criteria.getSourcePath(), response);
    }

    @Test
    public void testCompactFailure() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria("source/path", null, null);
        final CompactionResponse compactionResponse = new CompactionResponse(false, "source/path", FileType.TEXT);
        whenNew(CompactionManagerImpl.class).withArguments(any(Configuration.class), any(CompactionCriteria.class)).thenReturn(compactionManagerImpl);
        when(compactionManagerImpl.compact()).thenReturn(compactionResponse);
        final CompactionManagerInPlaceImpl compactionManager = new CompactionManagerInPlaceImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(false, FileType.TEXT, criteria.getSourcePath(), response);
    }

    @Test(expected = IllegalStateException.class)
    public void testUserDoesNotHaveWriteAccess() throws Exception {
        doThrow(new IllegalStateException()).when(fileSystem).access(new Path("/source/path"), FsAction.WRITE);
        final CompactionCriteria criteria = new CompactionCriteria("/source/path", null, null);
        final CompactionResponse compactionResponse = new CompactionResponse(true, "/source/path", FileType.TEXT);
        whenNew(CompactionManagerImpl.class).withArguments(any(Configuration.class), any(CompactionCriteria.class)).thenReturn(compactionManagerImpl);
        when(compactionManagerImpl.compact()).thenReturn(compactionResponse);
        final CompactionManagerInPlaceImpl compactionManager = new CompactionManagerInPlaceImpl(configuration, criteria);
        compactionManager.compact();
    }

    private void assertResponse(final boolean isSuccessful, final FileType fileType, final String targetPath,
                                final CompactionResponse actualResponse) {
        assertEquals(isSuccessful, actualResponse.isSuccessful());
        assertEquals(fileType, actualResponse.getTargetFileType());
        assertEquals(targetPath, actualResponse.getTargetPath());
    }
}
