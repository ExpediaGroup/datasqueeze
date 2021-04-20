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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.hotels.avro.compatibility.Compatibility;

import lombok.extern.slf4j.Slf4j;

/**
 * Mapper class for AVRO input file format.
 *
 * @author Samarth Kulkarni
 */
@Slf4j
public class AvroCompactionMapper extends Mapper<AvroKey<GenericRecord>, Object,
        Text, AvroValue<GenericRecord>> {

    private BaseMapper baseMapper;
    private Map<String, Boolean> compatibityMap;
    private Configuration configuration;

    /**
     * {@inheritDoc}
     */
    protected void setup(final Context context) throws IOException, InterruptedException {
        baseMapper = new BaseMapper(context);
        compatibityMap = new HashMap<String, Boolean>();
        configuration = context.getConfiguration();
    }

    @Override
    protected void map(final AvroKey<GenericRecord> key, final Object value, final Context context) throws IOException, InterruptedException {

        final FileSplit fileSplit = (FileSplit) context.getInputSplit();
        final Path filePath = fileSplit.getPath();
        AvroValue<GenericRecord> record = new AvroValue<GenericRecord>(key.datum());

        if (isValidData(key, filePath)) {
            final Text mapperKey = baseMapper.getKey(filePath.toString());
            context.write(mapperKey, record);
        }
    }

    private boolean isValidData(AvroKey<GenericRecord> key, Path filePath) throws IOException {
        if (compatibityMap.containsKey(filePath.toString())) {
            return compatibityMap.get(filePath.toString());
        }

        if (getHashOfData(key).equals(getHashOfDefaultObject())) {
            if (isSchemaCompatible(key.datum().getSchema(), filePath)) {
                compatibityMap.put(filePath.toString(), true);
                return true;
            } else {
                log.info("File {} is not compatible with input schema", filePath.toString());
                compatibityMap.put(filePath.toString(), false);
                return false;
            }
        }
        compatibityMap.put(filePath.toString(), true);
        return true;
    }

    private boolean isSchemaCompatible(Schema inputSchema, Path filePath) throws IOException {
        GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
        FileContext fc = FileContext.getFileContext(configuration);
        FileReader fileReader = DataFileReader.openReader(new AvroFSInput(fc, filePath), reader);
        Schema fileSchema = fileReader.getSchema();

        if (Compatibility.checkThat(fileSchema).canRead(inputSchema).getResult().getCompatibility().equals(patched.org.apache.avro.SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE)) {
            return true;
        }
        return false;
    }

    private String getHashOfData(AvroKey<GenericRecord> key) {
        return String.valueOf(key.datum().hashCode());
    }

    private String getHashOfDefaultObject() {
        return configuration.get("avro.default.data.hash");
    }

}

