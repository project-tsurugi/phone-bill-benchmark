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
        LOG.error("Unexpected application termination. Recording thread state for debugging. \n\n{}", dump.toString());
    }
}
