package com.expedia.edw.data.squeeze.impl;

import com.expedia.edw.data.squeeze.CompactionManagerFactory;
import com.expedia.edw.data.squeeze.models.FilePaths;
import com.expedia.edw.data.squeeze.models.FileType;
import lombok.extern.slf4j.Slf4j;
import net.sf.jmimemagic.Magic;
import net.sf.jmimemagic.MagicMatchNotFoundException;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.fs.*;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * File Manager class to provide file operations.
 *
 * @author Yashraj R. Sontakke
 */
@Slf4j
public class FileManager {

    private FileSystem fileSystem;
    public static final String SUCCESS = "_SUCCESS";

    public FileManager(final FileSystem fileSystem) {
        Validate.notNull(fileSystem, "FileSystem cannot be null");
        this.fileSystem = fileSystem;
    }

    /**
     * Retrieves list of all files to be compacted along with entire path with number of bytes to be compacted.
     *
     * @param sourceDirPath {@link Path} source directory path
     * @return {@link FilePaths}
     * @throws IOException
     */
    protected FilePaths getAllFilePaths(final Path sourceDirPath) throws IOException {
        final FilePaths filePaths = new FilePaths();
        final RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem.listFiles(sourceDirPath, true);
        while (fileStatusListIterator.hasNext()) {
            final LocatedFileStatus fileStatus = fileStatusListIterator.next();
            if (!SUCCESS.equalsIgnoreCase(fileStatus.getPath().getName())) {
                filePaths.addFile(fileStatus);
            }
            log.info("File {} of length {}", fileStatus.getPath(), fileStatus.getLen());
        }
        return filePaths;
    }

    /**
     * Returns the last source path
     *
     * @param paths list of {@link Path}
     * @return last source path
     */
    protected Path getLastSourceFilePath(final List<Path> paths) {
        if (!paths.isEmpty()) {
            return paths.get(paths.size() - 1);
        }
        return null;
    }

    /**
     * Determines the file type for the source path.
     *
     * @param sourceFilePaths list of source file paths
     * @return {@link FileType}
     * @throws Exception when input file format cannot be determined and source file paths is empty.
     */
    protected FileType getFileType(final List<Path> sourceFilePaths) throws Exception {
        if (!sourceFilePaths.isEmpty()) {
            final Path lastSourceFilePath = getLastSourceFilePath(sourceFilePaths);
            final FSDataInputStream fsDataInputStream = fileSystem.open(lastSourceFilePath);
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

    /**
     * Retrieves the number of reducers for the compacted data.
     *
     * @param bytes               the number of bytes to be compacted
     * @param maxReducersProvided max reducers provided by user
     * @return the number of reducers to be compacted.
     */
    protected Long getNumberOfReducers(final Long bytes, final Long maxReducersProvided) {
        if (bytes == null || bytes == 0L) {
            return 1L;
        }
        Long maxReducers = maxReducersProvided == null ? CompactionManagerFactory.MAX_REDUCERS : maxReducersProvided;
        Long reducers = bytes / CompactionManagerFactory.BYTES_PER_REDUCER + 1;
        return Math.min(reducers, maxReducers);
    }

    /**
     * Inspects if the source folder is data skewed. Returns skewed folder with reducers to run with.
     *
     * @param filePaths {@link FilePaths}
     * @return data skewed folder with reducers to run with.
     */
    protected JSONObject inspectDataSkew(final FilePaths filePaths) {
        final Map<String, Long> folderByBytes = new HashMap<String, Long>();
        for (Map.Entry<Path, Long> pathBySize : filePaths.getPathsByBytes().entrySet()) {
            final String parent = String.format("%s/", pathBySize.getKey().getParent().toString());
            if (!folderByBytes.containsKey(parent)) {
                folderByBytes.put(parent, pathBySize.getValue());
            } else {
                folderByBytes.put(parent, folderByBytes.get(parent) + pathBySize.getValue());
            }
        }
        final Long mean = filePaths.getBytes() / folderByBytes.size();
        final JSONObject dataSkew = new JSONObject();
        for (Map.Entry<String, Long> folder : folderByBytes.entrySet()) {
            if (folder.getValue() > CompactionManagerFactory.DATA_SKEW_FACTOR * mean) {
                Long reducers = getNumberOfReducers(folder.getValue(), null);
                dataSkew.put(folder.getKey(), reducers);
                log.info("DataSkew detected for folder {}", folder.getKey());
            }
        }
        return dataSkew;
    }
}
