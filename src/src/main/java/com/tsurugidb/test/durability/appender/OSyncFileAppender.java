/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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


