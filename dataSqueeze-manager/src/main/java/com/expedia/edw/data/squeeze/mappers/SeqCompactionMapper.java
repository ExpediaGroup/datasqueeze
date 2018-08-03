package com.expedia.edw.data.squeeze.mappers;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Mapper class for Sequence input file formats.
 *
 * @author Sai Sharan
 */
@Slf4j
public class SeqCompactionMapper extends Mapper<Object, Text, Text, Text> {

    private BaseMapper baseMapper;

    /**
     * {@inheritDoc}
     */
    protected void setup(Context context) throws IOException, InterruptedException {
        baseMapper = new BaseMapper(context);
    }

    /**
     * {@inheritDoc}
     */
    protected void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        if (value != null && value.toString() != null && value.toString().isEmpty()) {
            return;
        }
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        final Text mapperKey = baseMapper.getKey(fileSplit.getPath().toString());
        context.write(mapperKey, value);
    }
}
