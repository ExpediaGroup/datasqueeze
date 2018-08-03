package com.expedia.edw.data.squeeze.impl;

import com.expedia.edw.data.squeeze.CompactionManagerFactory;
import com.expedia.edw.data.squeeze.models.FilePaths;
import com.expedia.edw.data.squeeze.models.FileType;
import com.google.common.collect.ImmutableList;
import net.sf.jmimemagic.Magic;
import net.sf.jmimemagic.MagicMatch;
import org.apache.hadoop.fs.*;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyByte;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * Tests for {@link CompactionManagerImpl}
 *
 * @author Yashraj R. Sontakke
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileManager.class, FileSystem.class, FSDataInputStream.class, Magic.class, MagicMatch.class})
public class FileManagerTest {

    private final FileSystem fileSystem = mock(FileSystem.class);
    private final LocatedFileStatus fileStatus1 = mock(LocatedFileStatus.class);
    private final LocatedFileStatus fileStatus2 = mock(LocatedFileStatus.class);
    private final LocatedFileStatus fileStatus3 = mock(LocatedFileStatus.class);
    private final LocatedFileStatus fileStatus4 = mock(LocatedFileStatus.class);
    private final LocatedFileStatus fileStatus5 = mock(LocatedFileStatus.class);
    private final Path path1 = new Path("/source/path/file1");
    private final Path path2 = new Path("/source/path/file2");
    private final Path path3 = new Path("/source/path/file3");
    private final Path path4 = new Path("/source/path/file4");
    private final Path path5 = new Path("/source/path/file5");
    private final FileManager fileManager = new FileManager(fileSystem);

    @Before
    public void setup() throws Exception {
        CompactionManagerFactory.MAX_REDUCERS = 1000L;
        PowerMockito.mockStatic(FileSystem.class);

        when(fileStatus1.getPath()).thenReturn(path1);
        when(fileStatus1.isDirectory()).thenReturn(false);
        when(fileStatus1.getLen()).thenReturn(10000L);
        when(fileStatus2.getPath()).thenReturn(path2);
        when(fileStatus2.isDirectory()).thenReturn(false);
        when(fileStatus2.getLen()).thenReturn(20000L);
        when(fileStatus3.getPath()).thenReturn(path3);
        when(fileStatus3.isDirectory()).thenReturn(true);
        when(fileStatus4.getPath()).thenReturn(path4);
        when(fileStatus4.isDirectory()).thenReturn(false);
        when(fileStatus4.getLen()).thenReturn(40000L);
        when(fileStatus5.getPath()).thenReturn(path5);
        when(fileStatus5.isDirectory()).thenReturn(false);
        when(fileStatus4.getLen()).thenReturn(50000L);

        final FSDataInputStream fsDataInputStream = mock(FSDataInputStream.class);
        when(fileSystem.exists(path1)).thenReturn(true);
        when(fileSystem.open(path1)).thenReturn(fsDataInputStream);
    }

    @Test(expected = NullPointerException.class)
    public void testNullConfiguration() {
        new FileManager(null);
    }

    @Test
    public void testGetAllPathsSinglePath() throws IOException {
        LocatedFileStatus[] fileStatuses = {fileStatus1};
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = retrieveIterator(fileStatuses);
        when(fileSystem.listFiles(path1, true)).thenReturn(fileStatusRemoteIterator);
        FilePaths paths = fileManager.getAllFilePaths(path1);
        assertEquals(1, paths.getAllFilePaths().size());
        assertEquals(path1.getName(), paths.getAllFilePaths().get(0).getName());
        assertEquals(10000L, paths.getBytes(), 0);
    }

    @Test
    public void testGetAllPathsSinglePathWithSuccess() throws IOException {

        LocatedFileStatus[] fileStatuses = {fileStatus1};
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = retrieveIterator(fileStatuses);
        when(fileSystem.listFiles(path1, true)).thenReturn(fileStatusRemoteIterator);
        when(fileStatus1.getPath()).thenReturn(new Path("/source/_SUCCESS"));
        FilePaths paths = fileManager.getAllFilePaths(path1);
        assertTrue(paths.getAllFilePaths().isEmpty());
        assertEquals(0L, paths.getBytes(), 0);
    }

    @Test
    public void testGetAllPathsMultiplePaths() throws IOException {
        final FileManager fileManager = new FileManager(fileSystem);

        LocatedFileStatus[] fileStatuses = {fileStatus1, fileStatus2};
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = retrieveIterator(fileStatuses);
        when(fileSystem.listFiles(path1, true)).thenReturn(fileStatusRemoteIterator);
        FilePaths paths = fileManager.getAllFilePaths(path1);
        assertEquals(2, paths.getAllFilePaths().size());
        assertTrue(paths.getAllFilePaths().contains(path1));
        assertTrue(paths.getAllFilePaths().contains(path2));
        assertEquals(30000L, paths.getBytes(), 0);
    }

    @Test
    public void testGetAllPathsMultipleDirectories() throws IOException {
        when(fileStatus2.isDirectory()).thenReturn(true);
        final FileManager fileManager = new FileManager(fileSystem);
        LocatedFileStatus[] fileStatuses1 = {fileStatus1, fileStatus4, fileStatus5};
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator1 = retrieveIterator(fileStatuses1);
        when(fileSystem.listFiles(path1, true)).thenReturn(fileStatusRemoteIterator1);

        FilePaths paths = fileManager.getAllFilePaths(path1);
        assertEquals(3, paths.getAllFilePaths().size());
        assertTrue(paths.getAllFilePaths().contains(path1));
        assertTrue(paths.getAllFilePaths().contains(path4));
        assertTrue(paths.getAllFilePaths().contains(path5));
        assertEquals(60000L, paths.getBytes(), 0);
    }

    @Test
    public void testGetLastSourceFilePath() throws IOException {
        List<Path> paths = ImmutableList.of(path1, path2, path3);
        assertEquals(path3, fileManager.getLastSourceFilePath(paths));
    }

    @Test
    public void testGetLastSourceFilePathEmptyPaths() throws IOException {
        List<Path> paths = ImmutableList.of();
        assertNull(fileManager.getLastSourceFilePath(paths));
    }

    @Test(expected = IllegalStateException.class)
    public void testGetFileType() throws Exception {
        fileManager.getFileType(ImmutableList.<Path>of());
    }

    @Test
    public void testGetFileTypeOrc() throws Exception {
        whenNew(String.class).withArguments(anyByte()).thenReturn("ORC");
        assertEquals(FileType.ORC, fileManager.getFileType(ImmutableList.of(path1)));
    }

    @Test
    public void testGetFileTypeSeq() throws Exception {
        whenNew(String.class).withArguments(anyByte()).thenReturn("SEQ");
        assertEquals(FileType.SEQ, fileManager.getFileType(ImmutableList.of(path1)));
    }

    @Test
    public void testGetFileTypeText() throws Exception {
        whenNew(String.class).withArguments(anyByte()).thenReturn(" ");
        assertEquals(FileType.TEXT, fileManager.getFileType(ImmutableList.of(path1)));
    }

    @Test(expected = IllegalStateException.class)
    public void testGetFileTypeInvalidFileType() throws Exception {
        whenNew(String.class).withArguments(anyByte()).thenReturn(" ");
        final MagicMatch magicMatch = mock(MagicMatch.class);
        whenNew(MagicMatch.class).withNoArguments().thenReturn(magicMatch);
        when(magicMatch.getMimeType()).thenReturn("invalid");
        assertEquals(FileType.TEXT, fileManager.getFileType(ImmutableList.of(path1)));
    }

    @Test
    public void testGetNumberOfReducers() {
        CompactionManagerFactory.BYTES_PER_REDUCER = 1073741824L;
        CompactionManagerFactory.MAX_REDUCERS = 1000L;
        assertEquals(1L, fileManager.getNumberOfReducers(null, null), 0);
        assertEquals(1L, fileManager.getNumberOfReducers(0L, null), 0);
        assertEquals(1L, fileManager.getNumberOfReducers(100L, null), 0);
        assertEquals(4L, fileManager.getNumberOfReducers(3221225472L, null), 0);
        assertEquals(1000L, fileManager.getNumberOfReducers(10738491981824L, null), 0);

        assertEquals(1L, fileManager.getNumberOfReducers(null, 2000L), 0);
        assertEquals(1L, fileManager.getNumberOfReducers(0L, 2000L), 0);
        assertEquals(1L, fileManager.getNumberOfReducers(100L, 2000L), 0);
        assertEquals(4L, fileManager.getNumberOfReducers(3221225472L, 2000L), 0);
        assertEquals(2000L, fileManager.getNumberOfReducers(10738491981824L, 2000L), 0);
    }

    @Test
    public void testInspectDataSkew() {
        final FilePaths filePaths = new FilePaths();
        final LocatedFileStatus locatedFileStatus1 = mock(LocatedFileStatus.class);
        final LocatedFileStatus locatedFileStatus2 = mock(LocatedFileStatus.class);
        final LocatedFileStatus locatedFileStatus3 = mock(LocatedFileStatus.class);
        final LocatedFileStatus locatedFileStatus4 = mock(LocatedFileStatus.class);
        when(locatedFileStatus1.getLen()).thenReturn(1000L);
        when(locatedFileStatus2.getLen()).thenReturn(2000L);
        when(locatedFileStatus3.getLen()).thenReturn(2000L);
        when(locatedFileStatus4.getLen()).thenReturn(200000L);
        when(locatedFileStatus1.getPath()).thenReturn(new Path("/source/A/file1"));
        when(locatedFileStatus2.getPath()).thenReturn(new Path("/source/B/file2"));
        when(locatedFileStatus3.getPath()).thenReturn(new Path("/source/C/file3"));
        when(locatedFileStatus4.getPath()).thenReturn(new Path("/source/D/file4"));

        CompactionManagerFactory.DATA_SKEW_FACTOR = 1.3;
        CompactionManagerFactory.BYTES_PER_REDUCER = 1073741824L;
        CompactionManagerFactory.MAX_REDUCERS = 1000L;
        filePaths.addFile(locatedFileStatus1);
        filePaths.addFile(locatedFileStatus2);
        filePaths.addFile(locatedFileStatus3);

        JSONObject jsonObject = fileManager.inspectDataSkew(filePaths);
        assertTrue(jsonObject.keySet().isEmpty());

        filePaths.addFile(locatedFileStatus4);
        jsonObject = fileManager.inspectDataSkew(filePaths);
        System.out.println(jsonObject.toString());
        assertTrue(jsonObject.has("/source/D/"));
        assertEquals(1L, jsonObject.getLong("/source/D/"));

    }

    private RemoteIterator<LocatedFileStatus> retrieveIterator(final LocatedFileStatus[] fileStatuses) {
        return new RemoteIterator<LocatedFileStatus>() {
            int i = 0;

            public boolean hasNext() throws IOException {
                return i < fileStatuses.length;
            }

            public LocatedFileStatus next() throws IOException {
                return fileStatuses[i++];
            }
        };
    }
}
