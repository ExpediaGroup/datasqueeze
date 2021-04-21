/**
 * Copyright (C) 2017-2021 Expedia, Inc.
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
package com.expedia.dsp.data.squeeze.mappers;

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
