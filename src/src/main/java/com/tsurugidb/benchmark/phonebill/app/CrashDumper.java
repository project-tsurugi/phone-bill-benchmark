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
package com.tsurugidb.benchmark.phonebill.app;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * プロセス異常終了時にスレッドダンプをログに出力するためのクラス
 */
public class CrashDumper {

    private static boolean shutdownHookAdded = false;
    private static boolean outputThreadDumpOnExit = false;

    private static final Logger LOG = LoggerFactory.getLogger(CrashDumper.class);

    /**
     * プロセス終了時のスレッドダンプを有効にする.
     */
    public  static void enable() {
        outputThreadDumpOnExit = true;
        if (shutdownHookAdded) {
            return;
        }
        shutdownHookAdded = true;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // スレッドダンプ出力機能を呼び出す
            dumpThreads();
        }));
        LOG.debug("Shutdown hook added");
    }

    /**
     * 終了時のスレッドダンプを無効にする.
     *
     * 正常終了時にスレッドダンプをしたくない場合は、mainメソッドの最後で本メソッドを呼べば良い。
     */
    public static void disable() {
        outputThreadDumpOnExit = false;
    }

    private static void dumpThreads() {
        if (!outputThreadDumpOnExit) {
            return;
        }
        StringBuilder dump = new StringBuilder();
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);

        for (ThreadInfo threadInfo : threadInfos) {
            dump.append(threadInfo.toString()).append("\n");
        }
        LOG.debug("Unexpected application termination. Recording thread state for debugging. \n\n{}", dump.toString());
    }
}
