package com.tsurugidb.test.durability.appender;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.util.FileUtil;

public class OSyncFileAppender<E> extends FileAppender<E> {

    @Override
    public void openFile(String fileName) throws IOException {
        lock.lock();
        try {
            File file = new File(fileName);
            boolean result = FileUtil.createMissingParentDirectories(file);
            if (!result) {
                addError("Failed to create parent directories for [" + file.getAbsolutePath() + "]");
            }

            // Use Files.newOutputStream to open the file with O_SYNC option
            setOutputStream(Files.newOutputStream(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SYNC, append ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING));
        } finally {
            lock.unlock();
        }
    }
}


