package com.expedia.dsp.data.squeeze.impl;

import com.expedia.dsp.data.squeeze.CompactionManager;
import com.expedia.dsp.data.squeeze.CompactionManagerFactory;
import com.expedia.dsp.data.squeeze.impl.orc.OrcCombineFileInputFormat;
import com.expedia.dsp.data.squeeze.mappers.AvroCompactionMapper;
import com.expedia.dsp.data.squeeze.mappers.OrcCompactionMapper;
import com.expedia.dsp.data.squeeze.mappers.SeqCompactionMapper;
import com.expedia.dsp.data.squeeze.mappers.TextCompactionMapper;
import com.expedia.dsp.data.squeeze.models.CompactionCriteria;
import com.expedia.dsp.data.squeeze.models.FilePaths;
import com.expedia.dsp.data.squeeze.models.FileType;
import com.expedia.dsp.data.squeeze.reducers.OrcCompactionReducer;
import com.expedia.dsp.data.squeeze.SchemaSelector;
import com.expedia.dsp.data.squeeze.impl.text.TextCombineFileInputFormat;
import com.expedia.dsp.data.squeeze.mappers.BytesWritableCompactionMapper;
import com.expedia.dsp.data.squeeze.models.CompactionResponse;
import com.expedia.dsp.data.squeeze.reducers.AvroCompactionReducer;
import com.expedia.dsp.data.squeeze.reducers.BytesWritableCompactionReducer;
import com.expedia.dsp.data.squeeze.reducers.TextCompactionReducer;

import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
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
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.json.JSONObject;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static com.expedia.dsp.data.squeeze.models.FileType.AVRO;
import static com.expedia.dsp.data.squeeze.models.FileType.ORC;


/**
 * Implementation for {@link CompactionManager}.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class CompactionManagerImpl extends BaseCompactionManagerImpl {
    private static final String TEMP_OUTPUT_LOCATION = "/tmp/edw-compaction-utility-";
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
        final Path sourcePath = new Path(criteria.getSourcePath());
        final Path targetPath = new Path(criteria.getTargetPath());
        if (!fileSystem.exists(sourcePath)) {
            throw new IllegalStateException("Source location does not exist for compaction");
        }
        if (fileSystem.exists(targetPath)) {
            throw new IllegalStateException("Target location already exists. Please provide a location that does not exist");
        }
        final FileManager fileManager = new FileManager(fileSystem);
        final FilePaths allFilePaths = fileManager.getAllFilePaths(sourcePath);

        final List<Path> sourceFilePaths = allFilePaths.getAllFilePaths();
        final Long numberOfReducers = fileManager.getNumberOfReducers(allFilePaths.getBytes(), criteria.getMaxReducers());
        log.debug("Compacted files {}", sourceFilePaths.toString());
        log.info("Compacted bytes {}", allFilePaths.getBytes());
        log.info("Total number of reducers {}", numberOfReducers);
        FileType fileType = getFileType(fileManager, sourceFilePaths);
        log.info("Total number of files compacted {}", sourceFilePaths.size());
        log.info("Average File Size {}", allFilePaths.getBytes() / sourceFilePaths.size());
        log.info("Compaction performed for input path {}, output path {}, file type {}",
                criteria.getSourcePath(), criteria.getTargetPath(), fileType.name());

        final String thresholdInBytes = criteria.getThresholdInBytes() == null ?
                CompactionManagerFactory.DEFAULT_THRESHOLD_IN_BYTES.toString() : criteria.getThresholdInBytes().toString();
        log.info("Threshold in bytes {}", thresholdInBytes);
        configuration.set("compactionSourcePath", criteria.getSourcePath());
        configuration.set("compactionTargetPath", criteria.getTargetPath());
        configuration.set("compactionThreshold", thresholdInBytes);

        final JSONObject dataSkew = fileManager.inspectDataSkew(allFilePaths);
        if (!dataSkew.keySet().isEmpty()) {
            configuration.set("compactionDataSkew", dataSkew.toString());
        }

        Schema inputSchema = null;
        if (ORC == fileType) {
            final Reader reader = OrcFile.createReader(fileManager.getLastSourceFilePath(sourceFilePaths), OrcFile.readerOptions(configuration));
            final String schema = reader.getSchema().toString();
            log.info("ORC input file Schema " + schema);

            configuration.set("orc.mapred.map.output.value.schema", schema);
            configuration.set("orc.mapred.output.schema", schema);
            if (reader.getCompressionKind() != CompressionKind.NONE) {
                configuration.set("orc.compress", reader.getCompressionKind().name());
                log.info("ORC Compression {}", reader.getCompressionKind());
            }
        } else if (AVRO == fileType) {
            SchemaSelector schemaSelector = new SchemaSelectorImpl(criteria, fileSystem);
            String schemaStr = schemaSelector.getSchemaJSON();
            inputSchema = new Schema.Parser().parse(schemaStr);
            GenericRecordBuilder builder = new GenericRecordBuilder(inputSchema);
            GenericRecord rec = builder.build();
            configuration.set("avro.default.data.hash", String.valueOf(rec.hashCode()));
        }

        // Setup job configurations
        final Job job = Job.getInstance(configuration);
        job.setJarByClass(JobRunner.class);
        job.setJobName("Compaction-Data-Squeeze");
        job.setNumReduceTasks(numberOfReducers.intValue());
        for (final Path inputPath : sourceFilePaths) {
            FileInputFormat.addInputPath(job, inputPath);
        }
        log.info(UUID.randomUUID().toString());
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
                job.setInputFormatClass(TextCombineFileInputFormat.class);
                MultipleOutputs.addNamedOutput(job, NAMED_OUTPUT, TextOutputFormat.class, Text.class, Text.class);
                TextCombineFileInputFormat.setMaxInputSplitSize(job, CompactionManagerFactory.DEFAULT_THRESHOLD_IN_BYTES);
                break;
            case SEQ:

                final SequenceFile.Reader seqReader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(sourceFilePaths.get(0)));
                if (BYTES_WRITABLE.equalsIgnoreCase(seqReader.getValueClassName())) {
                    job.setMapperClass(BytesWritableCompactionMapper.class);
                    job.setMapOutputValueClass(BytesWritable.class);
                    job.setReducerClass(BytesWritableCompactionReducer.class);
                    job.setOutputValueClass(BytesWritable.class);
                    MultipleOutputs.addNamedOutput(job, NAMED_OUTPUT, SequenceFileOutputFormat.class, BytesWritable.class, BytesWritable.class);
                } else {
                    job.setMapperClass(SeqCompactionMapper.class);
                    job.setMapOutputValueClass(Text.class);
                    job.setReducerClass(TextCompactionReducer.class);
                    job.setOutputValueClass(Text.class);
                    MultipleOutputs.addNamedOutput(job, NAMED_OUTPUT, SequenceFileOutputFormat.class, BytesWritable.class, Text.class);
                }

                job.setMapOutputKeyClass(Text.class);
                job.setInputFormatClass(SequenceFileInputFormat.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputFormatClass(SequenceFileOutputFormat.class);
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
                job.setInputFormatClass(OrcCombineFileInputFormat.class);
                job.setReducerClass(OrcCompactionReducer.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(OrcValue.class);
                job.setOutputFormatClass(OrcOutputFormat.class);
                MultipleOutputs.addNamedOutput(job, NAMED_OUTPUT, OrcOutputFormat.class, NullWritable.class, OrcValue.class);
                OrcCombineFileInputFormat.setMaxInputSplitSize(job, CompactionManagerFactory.DEFAULT_THRESHOLD_IN_BYTES);
                break;
            case AVRO:
                job.setReducerClass(AvroCompactionReducer.class);
                job.setMapperClass(AvroCompactionMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(AvroValue.class);
                job.setInputFormatClass(AvroKeyInputFormat.class);
                job.setOutputFormatClass(AvroKeyOutputFormat.class);
                AvroJob.setOutputKeySchema(job, inputSchema);
                AvroJob.setInputKeySchema(job, inputSchema);
                AvroJob.setMapOutputValueSchema(job, inputSchema);
                configuration.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
                break;


            default:
                new IllegalStateException("Invalid File Type. Supported types TEXT, SEQ, ORC, AVRO");
        }
        String[] args = { criteria.getSourcePath(), criteria.getTargetPath() };
        final JobRunner jobRunner = new JobRunner(job);
        int res = ToolRunner.run(configuration, jobRunner, args);
        boolean success = res == 0;
        return new CompactionResponse(success, criteria.getTargetPath(), fileType);
    }

    private FileType getFileType(FileManager fileManager, List<Path> sourceFilePaths) throws Exception {

        FileType fileType;
        log.info("File Type in criteria {}", (criteria.getFileType() == null) ? "Empty" : criteria.getFileType());
        if (criteria.getFileType() == null) {
            fileType = fileManager.getFileType(sourceFilePaths);
        } else {
            fileType = FileType.valueOf(criteria.getFileType());
        }
        return fileType;
    }
}
