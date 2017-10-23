package com.expedia.edw.data.squeeze.impl;

import com.expedia.edw.data.squeeze.CompactionManager;
import com.expedia.edw.data.squeeze.CompactionManagerFactory;
import com.expedia.edw.data.squeeze.mappers.BytesWritableCompactionMapper;
import com.expedia.edw.data.squeeze.mappers.OrcCompactionMapper;
import com.expedia.edw.data.squeeze.mappers.TextCompactionMapper;
import com.expedia.edw.data.squeeze.models.CompactionCriteria;
import com.expedia.edw.data.squeeze.models.CompactionResponse;
import com.expedia.edw.data.squeeze.models.FileType;
import com.expedia.edw.data.squeeze.reducers.BytesWritableCompactionReducer;
import com.expedia.edw.data.squeeze.reducers.OrcCompactionReducer;
import com.expedia.edw.data.squeeze.reducers.TextCompactionReducer;
import lombok.extern.slf4j.Slf4j;
import net.sf.jmimemagic.Magic;
import net.sf.jmimemagic.MagicMatchNotFoundException;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.mapred.OrcValue;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.expedia.edw.data.squeeze.models.FileType.ORC;

/**
 * Implementation for {@link CompactionManager}.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class CompactionManagerImpl extends BaseCompactionManagerImpl {

    private static final String NAMED_OUTPUT = "Output";
    private static final String BYTES_WRITABLE = "org.apache.hadoop.io.BytesWritable";

    /**
     * {@inheritDoc}
     */
    public CompactionManagerImpl(final Configuration configuration, final CompactionCriteria criteria) {
        super(configuration, criteria);
    }

    /**
     * {@inheritDoc}
     */
    public void validate() {
        Validate.notBlank(criteria.getSourcePath(), "Source path cannot be null");
        Validate.notBlank(criteria.getTargetPath(), "Target path cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    public CompactionResponse compact() throws Exception {
        Validate.notNull(criteria, "Criteria cannot be null");
        if (criteria.getSourcePath().equalsIgnoreCase(criteria.getTargetPath())) {
            throw new IllegalStateException("Source and target path should be different for compaction");
        }
        log.info("Compaction requested for input path {}, output path {}", criteria.getSourcePath(), criteria.getTargetPath());
        final FileSystem fileSystem;
        if (criteria.getSourcePath().startsWith("s3")) {
            final URI uri = URI.create(criteria.getSourcePath());
            fileSystem = FileSystem.get(uri, configuration);
        } else {
            fileSystem = FileSystem.get(configuration);
        }
        final List<Path> sourceFilePaths = getAllFilePaths(new Path(criteria.getSourcePath()), fileSystem);
        log.debug("Compacted files {}", sourceFilePaths.toString());

        final FileType fileType = getFileType(sourceFilePaths, fileSystem);
        log.info("Compaction performed for input path {}, output path {}, file type {}",
                criteria.getSourcePath(), criteria.getTargetPath(), fileType.name());

        final String thresholdInBytes = criteria.getThresholdInBytes() == null ?
                CompactionManagerFactory.DEFAULT_THRESHOLD_IN_BYTES.toString() : criteria.getThresholdInBytes().toString();
        log.info("Threshold in bytes {}", thresholdInBytes);
        configuration.set("compactionSourcePath", criteria.getSourcePath());
        configuration.set("compactionTargetPath", criteria.getTargetPath());
        configuration.set("compactionThreshold", thresholdInBytes);

        if (ORC == fileType) {
            final Reader reader = OrcFile.createReader(sourceFilePaths.get(0), OrcFile.readerOptions(configuration));
            final String schema = reader.getSchema().toString();
            log.info("ORC input file Schema " + schema);

            configuration.set("orc.mapred.map.output.value.schema", schema);
            configuration.set("orc.mapred.output.schema", schema);
            if (reader.getCompressionKind() != CompressionKind.NONE) {
                configuration.set("orc.compress" , reader.getCompressionKind().name());
                log.info("ORC Compression {}", reader.getCompressionKind());
            }
        }

        // Setup job configurations
        final Job job = Job.getInstance(configuration);
        job.setJarByClass(JobRunner.class);
        for (final Path inputPath : sourceFilePaths) {
            FileInputFormat.addInputPath(job, inputPath);
        }
        final Path outputPath = new Path(TEMP_OUTPUT_LOCATION + UUID.randomUUID().toString());
        FileOutputFormat.setOutputPath(job, outputPath);

        switch (fileType) {
            case TEXT:
                job.setMapperClass(TextCompactionMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setReducerClass(TextCompactionReducer.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(Text.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                MultipleOutputs.addNamedOutput(job, NAMED_OUTPUT, TextOutputFormat.class, Text.class, Text.class);

                break;
            case SEQ:


                final  SequenceFile.Reader seqReader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(sourceFilePaths.get(0)));
                if (BYTES_WRITABLE.equalsIgnoreCase(seqReader.getValueClassName())) {
                    job.setMapperClass(BytesWritableCompactionMapper.class);
                    job.setMapOutputValueClass(BytesWritable.class);
                    job.setReducerClass(BytesWritableCompactionReducer.class);
                    job.setOutputValueClass(BytesWritable.class);
                    MultipleOutputs.addNamedOutput(job, NAMED_OUTPUT, SequenceFileOutputFormat.class, BytesWritable.class, BytesWritable.class);
                } else {
                    job.setMapperClass(TextCompactionMapper.class);
                    job.setMapOutputValueClass(Text.class);
                    job.setReducerClass(TextCompactionReducer.class);
                    job.setOutputValueClass(Text.class);
                    MultipleOutputs.addNamedOutput(job, NAMED_OUTPUT, SequenceFileOutputFormat.class, BytesWritable.class, Text.class);
                }

                job.setMapOutputKeyClass(Text.class);
                job.setInputFormatClass(SequenceFileInputFormat.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputFormatClass(SequenceFileOutputFormat.class);
                log.info("SEQ Value class type {}", seqReader.getValueClassName());
                log.info("SEQ Key class type {}", seqReader.getValueClass());
                if (seqReader.isCompressed()) {
                    log.info("SEQ Compression Codec {}", seqReader.getCompressionCodec().toString());
                    log.info("SEQ Compression Type {}", seqReader.getCompressionType().toString());
                    FileOutputFormat.setCompressOutput(job, true);
                    FileOutputFormat.setOutputCompressorClass(job, seqReader.getCompressionCodec().getClass());
                    SequenceFileOutputFormat.setOutputCompressionType(job, seqReader.getCompressionType());
                }
                break;
            case ORC:
                job.setMapperClass(OrcCompactionMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(OrcValue.class);
                job.setInputFormatClass(OrcInputFormat.class);
                job.setReducerClass(OrcCompactionReducer.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(OrcValue.class);
                job.setOutputFormatClass(OrcOutputFormat.class);
                MultipleOutputs.addNamedOutput(job, NAMED_OUTPUT, OrcOutputFormat.class, NullWritable.class, OrcValue.class);
                break;
            default:
                new IllegalStateException("Invalid File Type. Supported types TEXT, SEQ, ORC");
        }
        String[] args = {criteria.getSourcePath(), criteria.getTargetPath()};
        final JobRunner jobRunner = new JobRunner(job);
        int res = ToolRunner.run(configuration, jobRunner, args);
        boolean success = res == 0;
        return new CompactionResponse(success, criteria.getTargetPath(), fileType);
    }

    /**
     * Retrieves list of all files to be compacted along with entire path
     *
     * @param sourceDirPath {@link Path} source directory path
     * @param fileSystem    the {@link FileSystem}
     * @return list of {@link Path}
     * @throws IOException
     */
    private List<Path> getAllFilePaths(final Path sourceDirPath, final FileSystem fileSystem) throws IOException {
        final List<Path> filePaths = new ArrayList<Path>();
        for (final FileStatus fileStatus : fileSystem.listStatus(sourceDirPath)) {
            if (fileStatus.isDirectory()) {
                filePaths.addAll(getAllFilePaths(fileStatus.getPath(), fileSystem));
            } else {
                filePaths.add(fileStatus.getPath());
            }
        }
        return filePaths;
    }


    /**
     * Determines the file type for the source path.
     *
     * @param sourceFilePaths list of source file paths
     * @param fileSystem      {@link FileSystem}
     * @return {@link FileType}
     * @throws Exception when input file format cannot be determined and source file paths is empty.
     */
    private FileType getFileType(final List<Path> sourceFilePaths, FileSystem fileSystem) throws Exception {
        if (!sourceFilePaths.isEmpty()) {
            final FSDataInputStream fsDataInputStream = fileSystem.open(sourceFilePaths.get(0));
            FileType fileType = null;
            try {
                final byte[] header = new byte[1000];
                fsDataInputStream.read(header);
                final byte[] magicHeader = {header[0], header[1], header[2]};
                final String fileTypeString = new String(magicHeader);
                log.info("File header " + fileTypeString);
                if (FileType.ORC.toString().equals(fileTypeString)) {
                    fileType = FileType.ORC;
                } else if (FileType.SEQ.toString().equals(fileTypeString)) {
                    fileType = FileType.SEQ;
                } else {
                    final String mimeType = Magic.getMagicMatch(header, false).getMimeType();
                    log.info("File mime Type {}", mimeType);
                    if (FileType.TEXT.getValue().equalsIgnoreCase(mimeType)) {
                        log.info("name {}", FileType.TEXT.name());
                        fileType = FileType.TEXT;
                    }
                }
            } catch (MagicMatchNotFoundException e) {
                log.info("MagicMatch failed to find file type {}", e.toString());
            } finally {
                fsDataInputStream.close();
            }
            if (null != fileType) {
                return fileType;
            } else {
                throw new IllegalStateException("Input file format cannot be determined. Currently supported TEXT, ORC, SEQ");
            }
        }
        throw new IllegalStateException("Source Path does not have any files to compact.");
    }
}
