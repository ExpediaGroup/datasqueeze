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

import com.expedia.dsp.data.squeeze.impl.CombineFileWritable;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;

import java.io.IOException;

/**
 * Mapper class for ORC input file format.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class OrcCompactionMapper extends Mapper<CombineFileWritable, OrcStruct, Text, OrcValue> {

    private BaseMapper baseMapper;

    /**
     * {@inheritDoc}
     */
    protected void setup(final Context context) throws IOException, InterruptedException {
        baseMapper = new BaseMapper(context);
    }

    /**
     * {@inheritDoc}
     */
    protected void map(final CombineFileWritable key, final OrcStruct value, final Context context) throws IOException, InterruptedException {
        if (value != null && value.toString() != null && value.toString().isEmpty()) {
            return;
        }

        final OrcValue orcValue = new OrcValue();
        orcValue.value = value;

        final Text mapperKey = baseMapper.getKey(key.getFileName());
        context.write(mapperKey, orcValue);
    }
}
