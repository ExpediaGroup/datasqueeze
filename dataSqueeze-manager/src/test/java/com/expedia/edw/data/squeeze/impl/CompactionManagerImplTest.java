package com.expedia.edw.data.squeeze.impl;

import com.expedia.edw.data.squeeze.CompactionManagerFactory;
import com.expedia.edw.data.squeeze.models.CompactionCriteria;
import com.expedia.edw.data.squeeze.models.CompactionResponse;
import com.expedia.edw.data.squeeze.models.FileType;
import net.sf.jmimemagic.Magic;
import net.sf.jmimemagic.MagicMatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.ReaderImpl;
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
import static org.mockito.Matchers.anyByte;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * Tests for {@link CompactionManagerImpl}
 *
 * @author Yashraj R. Sontakke
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CompactionManagerImpl.class, FileSystem.class, FSDataInputStream.class, OrcFile.class, Reader.class, Job.class
        , Magic.class, MagicMatch.class, UUID.class})
public class CompactionManagerImplTest {

    private final Job job = mock(Job.class);
    private final Configuration configuration = mock(Configuration.class);
    private final JobRunner jobRunner = mock(JobRunner.class);
    private final FileSystem fileSystem = mock(FileSystem.class);
    private final FileStatus fileStatus = mock(FileStatus.class);
    private final FileStatus fileStatus2 = mock(FileStatus.class);
    private final Path path = new Path("/source/path/file");
    private final Path path2 = new Path("/source/path/dir");
    private final ReaderImpl reader = mock(ReaderImpl.class);
    private final SequenceFile.Reader seqReader = mock(SequenceFile.Reader.class);


    @Test(expected = NullPointerException.class)
    public void testNullConfiguration() {
        final CompactionCriteria criteria = new CompactionCriteria(null, "target/path", null);
        new CompactionManagerImpl(null, criteria);
    }

    @Test(expected = NullPointerException.class)
    public void testNullCriteria() throws Exception {
        new CompactionManagerImpl(configuration, null);
    }

    @Test(expected = NullPointerException.class)
    public void testNullSourcePath() {
        final CompactionCriteria criteria = new CompactionCriteria(null, "target/path", null);
        new CompactionManagerImpl(null, criteria);
    }

    @Test(expected = NullPointerException.class)
    public void testNullTargetPath() {
        final CompactionCriteria criteria = new CompactionCriteria("/source/path", null, null);
        new CompactionManagerImpl(null, criteria);
    }

    @Before
    public void setup() throws Exception {

        PowerMockito.mockStatic(FileSystem.class);

        when(FileSystem.get(configuration)).thenReturn(fileSystem);
        when(FileSystem.get(any(URI.class), any(Configuration.class))).thenReturn(fileSystem);
        when(fileStatus.getPath()).thenReturn(path);
        when(fileStatus.isDirectory()).thenReturn(false);
        FileStatus[] fileStatuses = {fileStatus};
        when(fileSystem.listStatus(any(Path.class))).thenReturn(fileStatuses);

        when(fileStatus2.getPath()).thenReturn(path2);
        when(fileStatus2.isDirectory()).thenReturn(true);
        when(fileSystem.listStatus(path2)).thenReturn(fileStatuses);

        final FSDataInputStream fsDataInputStream = mock(FSDataInputStream.class);
        when(fileSystem.open(any(Path.class))).thenReturn(fsDataInputStream);
        mockStatic(Job.class);
        when(Job.getInstance(configuration)).thenReturn(job);
        when(job.getConfiguration()).thenReturn(configuration);
        when(path.getFileSystem(configuration)).thenReturn(fileSystem);
        when(fileSystem.makeQualified(path)).thenReturn(path);
        final UUID uuid = UUID.randomUUID();
        mockStatic(UUID.class);
        whenNew(UUID.class).withAnyArguments().thenReturn(uuid);
        when(UUID.randomUUID()).thenReturn(uuid);
        whenNew(Path.class).withArguments("/apps/datasqueeze/staging/tmp-" + uuid.toString()).thenReturn(path);
        whenNew(Path.class).withArguments("/source/path").thenReturn(path);
        whenNew(Path.class).withArguments("s3/source/path").thenReturn(path);
        whenNew(Path.class).withArguments("/source/path/dir").thenReturn(path2);

        when(configuration.get("mapreduce.multipleoutputs", "")).thenReturn("");

        whenNew(ReaderImpl.class).withArguments(any(Path.class), any(OrcFile.ReaderOptions.class)).thenReturn(reader);
        final TypeDescription schema = TypeDescription.createStruct()
                .addField("field1", TypeDescription.createInt());
        when(reader.getSchema()).thenReturn(schema);
        when(reader.getCompressionKind()).thenReturn(CompressionKind.SNAPPY);
        CompactionManagerFactory.DEFAULT_THRESHOLD_IN_BYTES = 1234L;
        whenNew(JobRunner.class).withArguments(job).thenReturn(jobRunner);

        whenNew(SequenceFile.Reader.class).withArguments(any(Configuration.class), any(Path.class)).thenReturn(seqReader);
        when(seqReader.isCompressed()).thenReturn(true);
        CompressionCodec compressionCodec = mock(CompressionCodec.class);
        when(seqReader.getCompressionCodec()).thenReturn(compressionCodec);
        when(seqReader.getCompressionType()).thenReturn(SequenceFile.CompressionType.BLOCK);
    }

    @Test
    public void testCompactOrcHdfs() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria("/source/path", "target/path", null);
        whenNew(String.class).withArguments(anyByte()).thenReturn("ORC");
        final String[] args = {criteria.getSourcePath(), criteria.getTargetPath()};

        when(jobRunner.run(args)).thenReturn(0);
        final CompactionManagerImpl compactionManager = new CompactionManagerImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(true, FileType.ORC, criteria.getTargetPath(), response);
    }

    @Test
    public void testCompactOrcS3() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria("s3/source/path", "target/path", null);
        whenNew(String.class).withArguments(anyByte()).thenReturn("ORC");
        final String[] args = {criteria.getSourcePath(), criteria.getTargetPath()};

        when(jobRunner.run(args)).thenReturn(0);
        final CompactionManagerImpl compactionManager = new CompactionManagerImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(true, FileType.ORC, criteria.getTargetPath(), response);

    }

    @Test
    public void testCompactOrcNoCompression() throws Exception {
        when(reader.getCompressionKind()).thenReturn(CompressionKind.NONE);
        final CompactionCriteria criteria = new CompactionCriteria("s3/source/path", "target/path", null);
        whenNew(String.class).withArguments(anyByte()).thenReturn("ORC");
        final String[] args = {criteria.getSourcePath(), criteria.getTargetPath()};

        when(jobRunner.run(args)).thenReturn(0);
        final CompactionManagerImpl compactionManager = new CompactionManagerImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(true, FileType.ORC, criteria.getTargetPath(), response);

    }

    @Test
    public void testCompactSeq() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria("/source/path", "target/path", null);
        whenNew(String.class).withArguments(anyByte()).thenReturn("SEQ");
        final String[] args = {criteria.getSourcePath(), criteria.getTargetPath()};

        when(jobRunner.run(args)).thenReturn(0);
        final CompactionManagerImpl compactionManager = new CompactionManagerImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(true, FileType.SEQ, criteria.getTargetPath(), response);
    }

    @Test
    public void testCompactSeqBytesWritableValueClass() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria("/source/path", "target/path", null);
        when(seqReader.getValueClassName()).thenReturn("org.apache.hadoop.io.BytesWritable");
        whenNew(String.class).withArguments(anyByte()).thenReturn("SEQ");
        final String[] args = {criteria.getSourcePath(), criteria.getTargetPath()};

        when(jobRunner.run(args)).thenReturn(0);
        final CompactionManagerImpl compactionManager = new CompactionManagerImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(true, FileType.SEQ, criteria.getTargetPath(), response);
    }

    @Test
    public void testCompactSeqNoCompression() throws Exception {
        when(seqReader.isCompressed()).thenReturn(false);
        final CompactionCriteria criteria = new CompactionCriteria("/source/path", "target/path", null);
        whenNew(String.class).withArguments(anyByte()).thenReturn("SEQ");
        final String[] args = {criteria.getSourcePath(), criteria.getTargetPath()};

        when(jobRunner.run(args)).thenReturn(0);
        final CompactionManagerImpl compactionManager = new CompactionManagerImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(true, FileType.SEQ, criteria.getTargetPath(), response);
    }

    @Test
    public void testCompactText() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria("/source/path", "target/path", null);
        whenNew(String.class).withArguments(anyByte()).thenReturn(" ");
        final String[] args = {criteria.getSourcePath(), criteria.getTargetPath()};

        when(jobRunner.run(args)).thenReturn(0);
        final CompactionManagerImpl compactionManager = new CompactionManagerImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(true, FileType.TEXT, criteria.getTargetPath(), response);
    }

    @Test(expected = IllegalStateException.class)
    public void testCompactInvalidFileType() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria("/source/path/", "target/path", null);
        whenNew(String.class).withArguments(anyByte()).thenReturn(" ");
        final String[] args = {criteria.getSourcePath(), criteria.getTargetPath()};

        final MagicMatch magicMatch = mock(MagicMatch.class);
        whenNew(MagicMatch.class).withNoArguments().thenReturn(magicMatch);
        when(magicMatch.getMimeType()).thenReturn("invalid");
        when(jobRunner.run(args)).thenReturn(0);
        final CompactionManagerImpl compactionManager = new CompactionManagerImpl(configuration, criteria);
        compactionManager.compact();
    }

    @Test(expected = IllegalStateException.class)
    public void testCompactNoSourceFiles() throws Exception {
        final FileStatus[] fileStatuses = {};
        when(fileSystem.listStatus(any(Path.class))).thenReturn(fileStatuses);
        final CompactionCriteria criteria = new CompactionCriteria("/source/path/", "target/path", null);
        whenNew(String.class).withArguments(anyByte()).thenReturn(" ");
        final String[] args = {criteria.getSourcePath(), criteria.getTargetPath()};

        when(jobRunner.run(args)).thenReturn(0);
        final CompactionManagerImpl compactionManager = new CompactionManagerImpl(configuration, criteria);
        compactionManager.compact();
    }

    @Test
    public void testCompactMultipleSourceFiles() throws Exception {

        final FileStatus[] fileStatuses2 = {fileStatus, fileStatus2};
        final FileStatus[] fileStatuses = {fileStatus};

        when(fileSystem.listStatus(path)).thenReturn(fileStatuses2);
        when(fileSystem.listStatus(path2)).thenReturn(fileStatuses);

        final CompactionCriteria criteria = new CompactionCriteria("/source/path", "target/path", null);
        whenNew(String.class).withArguments(anyByte()).thenReturn(" ");
        final String[] args = {criteria.getSourcePath(), criteria.getTargetPath()};

        when(jobRunner.run(args)).thenReturn(0);
        final CompactionManagerImpl compactionManager = new CompactionManagerImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(true, FileType.TEXT, criteria.getTargetPath(), response);
    }


    @Test
    public void testCompactFailure() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria("/source/path", "target/path", null);
        whenNew(String.class).withArguments(anyByte()).thenReturn("SEQ");
        final String[] args = {criteria.getSourcePath(), criteria.getTargetPath()};

        when(jobRunner.run(args)).thenReturn(1);
        final CompactionManagerImpl compactionManager = new CompactionManagerImpl(configuration, criteria);
        final CompactionResponse response = compactionManager.compact();
        assertResponse(false, FileType.SEQ, criteria.getTargetPath(), response);
    }

    @Test(expected = IllegalStateException.class)
    public void testCompactSameSourceAndPathTarget() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria("/source/path", "/source/path", null);
        whenNew(String.class).withArguments(anyByte()).thenReturn("SEQ");
        final String[] args = {criteria.getSourcePath(), criteria.getTargetPath()};

        when(jobRunner.run(args)).thenReturn(1);
        final CompactionManagerImpl compactionManager = new CompactionManagerImpl(configuration, criteria);
        compactionManager.compact();
    }

    private void assertResponse(final boolean isSuccessful, final FileType fileType, final String targetPath,
                                final CompactionResponse actualResponse) {
        assertEquals(isSuccessful, actualResponse.isSuccessful());
        assertEquals(fileType, actualResponse.getTargetFileType());
        assertEquals(targetPath, actualResponse.getTargetPath());
    }

}
