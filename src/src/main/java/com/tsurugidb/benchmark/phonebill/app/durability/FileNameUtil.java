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
package com.tsurugidb.benchmark.phonebill.app.durability;

import java.nio.file.Path;
import java.nio.file.Paths;

public class FileNameUtil {

    public static Path buildFilePathWithClass(String prefix, Class<?> clazz) {
        String className = clazz.getSimpleName();
        if (className != null && !className.isEmpty()) {
            className = Character.toLowerCase(className.charAt(0)) + className.substring(1);
        }
        String fileName = prefix + "-" + className;
        return Paths.get(fileName);
    }
}
