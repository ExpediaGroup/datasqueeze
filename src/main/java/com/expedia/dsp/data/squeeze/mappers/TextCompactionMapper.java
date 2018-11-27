package com.expedia.dsp.data.squeeze.mappers;

import com.expedia.dsp.data.squeeze.impl.CombineFileWritable;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class for Text and Sequence input file formats.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class TextCompactionMapper extends Mapper<CombineFileWritable, Text, Text, Text> {

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
    protected void map(final CombineFileWritable key, final Text value, final Context context) throws IOException, InterruptedException {
        if (value != null && value.toString() != null && value.toString().isEmpty()) {
            return;
        }

        final Text mapperKey = baseMapper.getKey(key.getFileName());
        context.write(mapperKey, value);
    }
}
