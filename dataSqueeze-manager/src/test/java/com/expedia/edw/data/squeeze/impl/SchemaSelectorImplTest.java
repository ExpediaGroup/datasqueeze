package com.expedia.edw.data.squeeze.impl;

import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.expedia.edw.data.squeeze.models.CompactionCriteria;

/**
 * Tests for {@link SchemaSelectorImpl}
 *
 * @author Samarth Kulkarni
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({ FileSystem.class, FSDataInputStream.class, FileStatus.class, SchemaSelectorImpl.class, Schema.Parser.class })
public class SchemaSelectorImplTest {

    private final FileSystem fileSystem = mock(FileSystem.class);
    private final FSDataInputStream fsDataInputStream = mock(FSDataInputStream.class);
    private final InputStream in = mock(InputStream.class);
    private final Path path = new Path("/schema/path/file");
    private final Schema schema = mock(Schema.class);
    private final FileStatus fileStatus = mock(FileStatus.class);
    private final Schema.Parser parser = mock(Schema.Parser.class);
    private final InputStream inputStream = mock(InputStream.class);
    private final DataFileStream<Object> avroDataStream = mock(DataFileStream.class);


    @Before
    public void setup() throws Exception {
        PowerMockito.mockStatic(FileSystem.class);
    }

    @Test
    public void testGetSchemaJSON() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria(null, "target/path", null, null, "AVRO", "schema/path/file");
        SchemaSelectorImpl schemaSelector = new SchemaSelectorImpl(criteria, fileSystem);
        whenNew(Schema.Parser.class).withNoArguments().thenReturn(parser);
        when(parser.parse(any(FSDataInputStream.class))).thenReturn(schema);
        when(fileSystem.open(any(Path.class))).thenReturn(fsDataInputStream);
        when(schema.toString()).thenReturn("{schemaString}");

        String schemaStr = schemaSelector.getSchemaJSON();
        Assert.assertTrue(schemaStr != null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetSchemaJSONPathErr() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria(null, "target/path", null, null, "AVRO", "schema/path/file");
        SchemaSelectorImpl schemaSelector = new SchemaSelectorImpl(criteria, fileSystem);
        whenNew(Schema.Parser.class).withNoArguments().thenReturn(parser);
        when(parser.parse(any(FSDataInputStream.class))).thenReturn(schema);
        when(fileSystem.open(any(Path.class))).thenThrow(new IOException());

        String schemaStr = schemaSelector.getSchemaJSON();
        Assert.assertNull(schemaStr);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetSchemaJSONSchemaErr() throws Exception {
        final CompactionCriteria criteria = new CompactionCriteria(null, "target/path", null, null, "AVRO", "schema/path/file");
        SchemaSelectorImpl schemaSelector = new SchemaSelectorImpl(criteria, fileSystem);
        whenNew(Schema.Parser.class).withNoArguments().thenReturn(parser);
        when(parser.parse(any(FSDataInputStream.class))).thenThrow(new IOException());
        when(fileSystem.open(any(Path.class))).thenReturn(fsDataInputStream);

        String schemaStr = schemaSelector.getSchemaJSON();
        Assert.assertNull(schemaStr);
    }
}
