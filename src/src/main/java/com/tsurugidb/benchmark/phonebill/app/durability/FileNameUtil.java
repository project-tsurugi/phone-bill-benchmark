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
