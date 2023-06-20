package com.tsurugidb.benchmark.phonebill.app;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;
import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsurugidbWatcher implements Runnable  {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugidbWatcher.class);

    private static final String SERVER_NAME = "libexec/tsurugidb";
    private static final Path PROC = Path.of("/proc");

    private AtomicBoolean stopRequested = new AtomicBoolean(false);

    private long vsz = -1;
    private long rss = -1;


    @Override
    public void run() {
        Thread.currentThread().setName("TsurugidbWatcher");
        final NumberFormat fmt = NumberFormat.getNumberInstance(Locale.US);

        int pid = findServerPid();
        if (pid == -1) {
            LOG.error("Unable to locate tsurugidb. Terminating TsurugidbWatcher.");
            return;
        }
        Path path = PROC.resolve(Integer.toString(pid)).resolve("status");


        // 1秒に1回サーバのメモリ容量を出力する
        for (;;) {
            try {
                for (String line : Files.readAllLines(path)) {
                    if (line.startsWith("VmSize:")) {
                        vsz = parseMemoryValue(line);
                    } else if (line.startsWith("VmRSS:")) {
                        rss = parseMemoryValue(line);
                    }
                }
            } catch (IOException e) {
                String msg = "Unable to retrieve memory info for tsurugidb. It is possible that the server has crashed.";
                LOG.error(msg, e);
                throw new UncheckedIOException(e);
            }
            LOG.debug("tsurugidb memory info: VSZ = {} bytes, RSS = {} bytes", fmt.format(vsz), fmt.format(rss));
            if (stopRequested.get()) {
                LOG.debug("Stop requested for TsurugidbWatcher");
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


    /**
     * /procを調べてサーバのプロセスIDを取得する
     *
     * @return サーバのプロセスID、見つからなかったときは -1
     */
    private int findServerPid() {
        String me = System.getProperty("user.name");

        List<Path> procDirs = null;
        try {
            if(!Files.exists(PROC)) {
                LOG.debug("Directory not found: {}", PROC.toString());
                return -1;
            }
            procDirs = Files.list(PROC).filter(p -> Files.isDirectory(p)).collect(Collectors.toList());
            LOG.debug("{} directories found in {}", procDirs.size(), PROC.toString());
        } catch (IOException e) {
            LOG.warn("IOError", e);
            return -1;
        }
        for (Path dir: procDirs) {
            Path cmdline = dir.resolve("cmdline");
            LOG.trace("Checking direcory: {}", dir.toAbsolutePath().toString());
            try {
                if (!Files.readString(cmdline).contains(SERVER_NAME)) {
                    continue;
                }
                Path p = dir.getFileName();
                int pid =  p == null ? -1 : Integer.parseInt(p.toString());
                if (pid == -1) {
                    continue;
                }
                UserPrincipal owner = Files.getOwner(cmdline);
                if (!owner.getName().equals(me)) {
                    LOG.info("Found tsurugidb pid = {}, bad owner is not {}", pid, me);
                } else {
                    LOG.info("Found tsurugidb pid = {}", pid);
                    return pid;
                }
            } catch (IOException e) {
                // cmdlineを読み取れない => 他のユーザのプロセス or 存在しないプロセス => 無視する
                continue;
            }
        }
        return -1;
    }



    public void stop() {
        stopRequested.set(true);
    }

    /**
     * プロセスステータスの行からメモリサイズを抽出するメソッド
     *
     * @param line プロセスステータスの行
     * @return メモリサイズ（バイト単位）
     */
    static long parseMemoryValue(String line) {
        long value = -1;
        String[] parts = line.split("\\s+");
        if (parts.length == 2) {
            try {
                value = Long.parseLong(parts[1]);
            } catch (NumberFormatException e) {
                // ignore
            }
        } else if (parts.length == 3 && parts[2].equals("kB")) {
            try {
                value = Long.parseLong(parts[1]) * 1024;
            } catch (NumberFormatException e) {
                // ignore
            }
        } else if (parts.length == 3 && parts[2].equals("mB")) {
            try {
                value = Long.parseLong(parts[1]) * 1024 * 1024;
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return value;
    }


    /**
     * @return vsz
     */
    public long getVsz() {
        return vsz;
    }


    /**
     * @return rss
     */
    public long getRss() {
        return rss;
    }
}
