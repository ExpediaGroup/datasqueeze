package com.expedia.dsp.data.squeeze;

import com.expedia.dsp.data.squeeze.models.CompactionResponse;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to run compaction through CLI
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class Utility {

    private void utility(String... args) throws Exception {

        final Options options = new Options();
        options.addOption("sp", true, "sourcePath");
        options.addOption("tp", true, "targetPath");
        options.addOption("threshold", true, "thresholdInBytes");
        options.addOption("maxReducers", true, "maxReducers");
        options.addOption("fileType", true, "fileType");
        options.addOption("schemaPath", true, "schemaPath");


        final CommandLineParser commandLineParser = new GnuParser();
        final CommandLine commandLine = commandLineParser.parse(options, args);
        log.info("source Path " + commandLine.getOptionValue("sp"));
        log.info("Target Path " + commandLine.getOptionValue("tp"));
        log.info("Threshold " + commandLine.getOptionValue("threshold"));
        log.info("Max reducers " + commandLine.getOptionValue("maxReducers"));
        log.info("File type " + commandLine.getOptionValue("filesType"));
        log.info("Schema Path" + commandLine.getOptionValue("schemaPath"));


        final Map<String, String> optionsMap = new HashMap<String, String>();
        optionsMap.put("sourcePath", commandLine.getOptionValue("sp"));
        optionsMap.put("targetPath", commandLine.getOptionValue("tp"));
        optionsMap.put("thresholdInBytes", commandLine.getOptionValue("threshold"));
        optionsMap.put("maxReducers", commandLine.getOptionValue("maxReducers"));
        optionsMap.put("fileType", commandLine.getOptionValue("fileType"));
        optionsMap.put("schemaPath", commandLine.getOptionValue("schemaPath"));

        final CompactionManager compactionManager = CompactionManagerFactory.create(optionsMap);
        final CompactionResponse compactionResponse = compactionManager.compact();
        log.info("Compaction Response Success {}", compactionResponse.isSuccessful());
        log.info("Compaction Response FileType {}", compactionResponse.getTargetFileType());
        log.info("Compaction Response Target Path {}", compactionResponse.getTargetPath());
    }


    public static void main(String[] args) throws Exception {
        final Utility utility = new Utility();
        utility.utility(args);
    }
}
