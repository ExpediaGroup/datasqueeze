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

import com.expedia.dsp.data.squeeze.CompactionManagerFactory;
import com.expedia.dsp.data.squeeze.models.CompactionCriteria;
import com.expedia.dsp.data.squeeze.models.CompactionResponse;
import com.expedia.dsp.data.squeeze.models.FilePaths;
import com.expedia.dsp.data.squeeze.models.FileType;
import net.sf.jmimemagic.Magic;
import net.sf.jmimemagic.MagicMatch;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;
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
    private final FileManager fileManager = mock(FileManager.class);
    private final List<Path> filePaths1 = new ArrayList<Path>();
    private final Path path = new Path("/source/path/file");
    private final Path path2 = new Path("/source/path/dir");
    final String sourcePath = "/source/path";
    final String s3SourcePath = "s3/source/path";
    private final FilePaths filePaths = mock(FilePaths.class);

    @Test(expected = NullPointerException.class)
    public void testNullConfiguration() {
        final CompactionCriteria criteria = new CompactionCriteria(null, "target/path", null, null);
        new CompactionManagerInPlaceImpl(null, criteria);
    }

    @Test(expected = NullPointerException.class)
    public void testNullCriteria() throws Exception {
        new CompactionManagerInPlaceImpl(configuration, null);
    }

    @Test(expected = NullPointerException.class)
    public void testNullSourcePath() {
        final CompactionCriteria criteria = new CompactionCriteria(null, null, null, null);
        new CompactionManagerImpl(null, criteria);
    }

    /*@Test(expected = NullPointerException.class)
    public void testNullTempPath() {
        final CompactionCriteria criteria = new CompactionCriteria("/source/path", null, null, null, false, null);
        new CompactionManagerImpl(null, criteria);
    }*/

    @Test(expected = NullPointerException.class)
    public void testCompactSameSourceAndPathTarget() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria(sourcePath, sourcePath, null, null);

        final CompactionManagerInPlaceImpl compactionManager = new CompactionManagerInPlaceImpl(configuration, criteria);
        compactionManager.compact();
    }

    @Before
    public void setup() throws Exception {

        PowerMockito.mockStatic(FileSystem.class);
        when(FileSystem.get(configuration)).thenReturn(fileSystem);
        when(FileSystem.get(any(URI.class), any(Configuration.class))).thenReturn(fileSystem);
        CompactionManagerFactory.DEFAULT_THRESHOLD_IN_BYTES = 1234L;
        when(fileSystem.exists(path)).thenReturn(true);
        when(fileSystem.exists(path2)).thenReturn(true);
        when(fileSystem.exists(new Path(sourcePath))).thenReturn(true);
        when(fileSystem.exists(new Path(s3SourcePath))).thenReturn(true);
        when(fileSystem.rename(new Path(sourcePath), new Path("target/path"))).thenReturn(true);
        when(fileSystem.rename(new Path(s3SourcePath), new Path("target/path"))).thenReturn(true);
        whenNew(FileManager.class).withArguments(fileSystem).thenReturn(fileManager);
        filePaths1.add(path2);
        when(fileManager.getAllFilePaths(new Path(sourcePath))).thenReturn(filePaths);
        when(fileManager.getAllFilePaths(new Path(s3SourcePath))).thenReturn(filePaths);
        when(fileManager.getFileType(filePaths1)).thenReturn(FileType.TEXT);
        when(filePaths.getAllFilePaths()).thenReturn(filePaths1);
    }

    @Test(expected = IllegalStateException.class)
    public void testSourcePathDoesNotExist() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria(sourcePath, "target/path", null, null);
        when(fileSystem.exists(new Path(sourcePath))).thenReturn(false);
        final CompactionManagerInPlaceImpl compactionManager = new CompactionManagerInPlaceImpl(configuration, criteria);
        compactionManager.compact();
    }

    @Test(expected = NullPointerException.class)
    public void testTargetPathAlreadyExists() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria(sourcePath, "target/path", null, null);
        when(fileSystem.exists(new Path("target/path"))).thenReturn(true);
        final CompactionManagerInPlaceImpl compactionManager = new CompactionManagerInPlaceImpl(configuration, criteria);
        compactionManager.compact();
    }

    @Test(expected = NullPointerException.class)
    public void testNoSourceFilesToCompact() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria(sourcePath, "target/path", null, null);
        Pair<Long, List<Path>> filePathsWithBytes = new Pair(10000L, filePaths);
        when(fileManager.getAllFilePaths(new Path(sourcePath))).thenReturn(filePaths);
        final CompactionManagerInPlaceImpl compactionManager = new CompactionManagerInPlaceImpl(configuration, criteria);
        compactionManager.compact();
    }

    @Test
    public void testCompactOnPrem() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria(sourcePath, "target/path", null, null);
        final CompactionResponse compactionResponse = new CompactionResponse(true, "/source/path", FileType.TEXT);
        whenNew(CompactionManagerImpl.class).withArguments(any(Configuration.class), any(CompactionCriteria.class)).thenReturn(compactionManagerImpl);
        when(compactionManagerImpl.compact()).thenReturn(compactionResponse);
        final CompactionManagerInPlaceImpl compactionManager = new CompactionManagerInPlaceImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(true, FileType.TEXT, criteria.getSourcePath(), response);
    }

    @Test
    public void testCompactOnS3() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria(s3SourcePath, "target/path", null, null);
        final CompactionResponse compactionResponse = new CompactionResponse(true, "s3/source/path", FileType.TEXT);
        whenNew(CompactionManagerImpl.class).withArguments(any(Configuration.class), any(CompactionCriteria.class)).thenReturn(compactionManagerImpl);
        when(compactionManagerImpl.compact()).thenReturn(compactionResponse);
        final CompactionManagerInPlaceImpl compactionManager = new CompactionManagerInPlaceImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(true, FileType.TEXT, criteria.getSourcePath(), response);
    }

    @Test
    public void testCompactRetainTempFiles() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria("s3/source/path", "target/path", null, null);
        final CompactionResponse compactionResponse = new CompactionResponse(true, "s3/source/path", FileType.TEXT);
        whenNew(CompactionManagerImpl.class).withArguments(any(Configuration.class), any(CompactionCriteria.class)).thenReturn(compactionManagerImpl);
        when(compactionManagerImpl.compact()).thenReturn(compactionResponse);
        final CompactionManagerInPlaceImpl compactionManager = new CompactionManagerInPlaceImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(true, FileType.TEXT, criteria.getSourcePath(), response);
    }

    @Test
    public void testCompactWithThreshold() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria("s3/source/path", "target/path", 10L, null);
        final CompactionResponse compactionResponse = new CompactionResponse(true, "s3/source/path", FileType.TEXT);
        whenNew(CompactionManagerImpl.class).withArguments(any(Configuration.class), any(CompactionCriteria.class)).thenReturn(compactionManagerImpl);
        when(compactionManagerImpl.compact()).thenReturn(compactionResponse);
        final CompactionManagerInPlaceImpl compactionManager = new CompactionManagerInPlaceImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(true, FileType.TEXT, criteria.getSourcePath(), response);
    }

    @Test
    public void testCompactWithMaxReducers() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria("s3/source/path", "target/path", null, null);
        final CompactionResponse compactionResponse = new CompactionResponse(true, "s3/source/path", FileType.TEXT);
        whenNew(CompactionManagerImpl.class).withArguments(any(Configuration.class), any(CompactionCriteria.class)).thenReturn(compactionManagerImpl);
        when(compactionManagerImpl.compact()).thenReturn(compactionResponse);
        final CompactionManagerInPlaceImpl compactionManager = new CompactionManagerInPlaceImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(true, FileType.TEXT, criteria.getSourcePath(), response);
    }

    @Test
    public void testCompactFailure() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria(sourcePath,  "target/path", null, null);
        final CompactionResponse compactionResponse = new CompactionResponse(false, sourcePath, FileType.TEXT);
        whenNew(CompactionManagerImpl.class).withArguments(any(Configuration.class), any(CompactionCriteria.class)).thenReturn(compactionManagerImpl);
        when(compactionManagerImpl.compact()).thenReturn(compactionResponse);
        final CompactionManagerInPlaceImpl compactionManager = new CompactionManagerInPlaceImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(false, FileType.TEXT, criteria.getSourcePath(), response);
    }

    private void assertResponse(final boolean isSuccessful, final FileType fileType, final String targetPath,
                                final CompactionResponse actualResponse) {
        assertEquals(isSuccessful, actualResponse.isSuccessful());
        assertEquals(fileType, actualResponse.getTargetFileType());
        assertEquals(targetPath, actualResponse.getTargetPath());
    }
}
