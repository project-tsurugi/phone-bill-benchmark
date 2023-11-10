package com.tsurugidb.benchmark.phonebill.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config.DbmsType;
import com.tsurugidb.benchmark.phonebill.app.Config.IsolationLevel;
import com.tsurugidb.benchmark.phonebill.app.Config.TransactionOption;
import com.tsurugidb.benchmark.phonebill.app.Config.TransactionScope;
import com.tsurugidb.benchmark.phonebill.app.billing.PhoneBill;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager.CounterKey;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager.CounterName;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.TxOption.Table;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.online.TxStatistics;
import com.tsurugidb.benchmark.phonebill.online.TxStatistics.TxStatisticsBundle;
import com.tsurugidb.benchmark.phonebill.testdata.CreateTestData;

/**
 * 引数でしていされた複数の設定でバッチを実行する。
 *
 */
public class MultipleExecute extends ExecutableCommand {
    private static final String ENV_NAME = "DB_INIT_CMD";

    private static final Logger LOG = LoggerFactory.getLogger(MultipleExecute.class);
    private List<Record> records = new ArrayList<>();
    private Set<History> expectedHistories;
    private Set<Billing> expectedBillings;
    private String onlineAppReport = "# Online Application Report \n\n";

    public static void main(String[] args) throws Exception {
        MultipleExecute threadBench = new MultipleExecute();
        List<ConfigInfo> configInfos = createConfigInfos(args, 0);
        CrashDumper.enable();
        threadBench.execute(configInfos);
        CrashDumper.disable();
    }

    @Override
    public void execute(List<ConfigInfo> configInfos) throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(1);
        TsurugidbWatcher task = null;
        Future<?> future = null;
        try {
            Record.resetBaseElapsedMillis();
            boolean prevConfigHasOnlineApp = false;
            for (ConfigInfo info : configInfos) {
                Config config = info.config;
                LOG.info("Using config {} ",info.configPath.toAbsolutePath().toString());
                LOG.debug("Config is " + System.lineSeparator() + "--- " + System.lineSeparator() + config
                        + System.lineSeparator() + "---", info.configPath.toAbsolutePath().toString());
                if (config.dbmsType.isTsurugi()) {
                    dbiInit();
                    task = new TsurugidbWatcher();
                    future = service.submit(task);
                }
                initTestData(config, prevConfigHasOnlineApp);
                TxStatistics.clear();
                Record record = new Record(config);
                records.add(record);
                record.start();
                PhoneBill phoneBill = new PhoneBill();
                phoneBill.execute(config);
                record.finish(config, phoneBill.getTryCount(), phoneBill.getAbortCount());
                if (!config.hasOnlineApp()) {
                    record.setNumberOfDiffrence(checkResult(config));
                }
                if (config.dbmsType.isTsurugi()) {
                    LOG.info("Sending a request to stop TsurugidbWatcher.");
                    task.stop();
                    future.get();
                    LOG.info("TsurugidbWatcher was stopped.");
                    record.setMemInfo(task.getVsz(), task.getRss());
                }
                writeResult(config);
                if (config.hasOnlineApp()) {
                    writeOnlineAppReport(config);
                }

                prevConfigHasOnlineApp = config.hasOnlineApp();
                PhoneBillDbManager.reportNotClosed();
            }
        } finally {
            if (task != null) {
                task.stop();
            }
            if (future != null) {
                future.get();
            }
            service.shutdown();
        }
    }

    /**
     * テストデータを初期化する
     *
     * @param config
     * @param prevConfigHasOnlineApp
     * @throws Exception
     */
    public void initTestData(Config config, boolean prevConfigHasOnlineApp) throws Exception {
        boolean needCreateTestData = needCreateTestData(config);
        if (prevConfigHasOnlineApp || needCreateTestData) {
            LOG.info("Starting test data generation.");
            new CreateTable().execute(config);
            new CreateTestData().execute(config);
            LOG.info("Test data generation has finished.");
        } else {
            LOG.info("Starting test data update.");
            try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
                manager.execute(TxOption.ofLTX(0, TxLabel.BATCH_INITIALIZE, Table.BILLING, Table.HISTORY), () -> {
                    manager.getHistoryDao().updateChargeNull();
                    manager.getBillingDao().delete();
                });
                LOG.info("Test data update has finished.");
            }
        }
    }

    public boolean needCreateTestData(Config config) {
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            // テーブルの存在確認
            Ddl ddl = manager.getDdl();
            if (!ddl.tableExists("billing") || !ddl.tableExists("contracts") || !ddl.tableExists("history")) {
                return true;
            }
            long countHistory = manager.execute(TxOption.of(), () -> {
                return manager.getHistoryDao().count();
            });
            LOG.debug("countHistory = {}", countHistory);
            if (countHistory != config.numberOfHistoryRecords) {
                return true;
            }
            long countContracts = manager.execute(TxOption.of(), () -> {
                return manager.getContractDao().count();
            });
            LOG.debug("countContracts = {}", countContracts);
            if (countContracts != config.numberOfContractsRecords) {
                return true;
            }
        }
        return false;
    }


    /**
     * 環境変数"DB_INIT_CMD"が設定されている場合、環境変数で指定されたコマンドを実行する
     *
     * @throws IOException
     * @throws InterruptedException
     */
    private void dbiInit() throws IOException, InterruptedException {
        LOG.info("Enter to dbInit().");
        String cmd = System.getenv(ENV_NAME);
        if (cmd == null || cmd.isEmpty()) {
            return;
        }
        LOG.info("Executing command: {}.", cmd);
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(p.getInputStream(), Charset.defaultCharset()))) {
            String line;
            while ((line = r.readLine()) != null) {
                System.out.println(line);
            }
        }
        int retCode = p.waitFor();
        String msg = cmd + " was terminated with exit code " + retCode + ".";
        LOG.info(msg);
        if (retCode != 0) {
            throw new RuntimeException(msg);
        }
    }


    /**
     * 結果をCSVに出力する
     *
     * @param config
     * @throws IOException
     */
    private void writeResult(Config config) throws IOException {
        Path outputPath = Paths.get(config.reportDir).resolve("result.csv");
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(outputPath))) {
            pw.println(Record.header());
            records.stream().forEach(r -> pw.println(r.toString()));
        }
    }

    /**
     * オンラインアプリのレポートを出力する
     *
     * @param config
     * @param record
     */
    private void writeOnlineAppReport(Config config) {
        // ex: ICEAXE-OCC-
        String title = createTitile(config);
        String baselineTitle = createBaselineTitile(config);
        Path outputPath = Paths.get(config.reportDir).resolve("online-app.md");
        try {
            LOG.debug("Creating an online application report for {}", title);
            String newReport = createOnlineAppReport(config, title, baselineTitle);
            LOG.debug("Online application report: {}", newReport);
            onlineAppReport = onlineAppReport + newReport;
            LOG.debug("Writing online application reports to {}", outputPath.toAbsolutePath().toString());
            Files.writeString(outputPath, onlineAppReport);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static String createTitile(Config config) {
        int onlineThreadCount = getOnlineAppThreadCount(config);
        String title = String.format("%s-%s-%s-ONLINE-T%02d-BATCH-T%02d",
                config.dbmsType,
                config.transactionOption,
                config.transactionScope,
                onlineThreadCount,
                config.onlineOnly ? 0 : config.threadCount);
        return title;
    }

    static int getOnlineAppThreadCount(Config config) {
        int onlineThreadCount = 0;
        onlineThreadCount += getThreadCount(config.historyInsertTransactionPerMin, config.historyInsertThreadCount);
        onlineThreadCount += getThreadCount(config.historyUpdateRecordsPerMin, config.historyUpdateThreadCount);
        onlineThreadCount += getThreadCount(config.masterDeleteInsertRecordsPerMin, config.masterDeleteInsertThreadCount);
        onlineThreadCount += getThreadCount(config.masterUpdateRecordsPerMin, config.masterUpdateThreadCount);
        return onlineThreadCount;
    }

    static String createBaselineTitile(Config config) {
        Config newConfig = config.clone();
        newConfig.threadCount = 0;
        return createTitile(newConfig);
    }

    private static int getThreadCount(int tpm, int threadCount) {
        if (tpm != 0 && threadCount > 0) {
            return threadCount;
        }
        return 0;
    }



    /**
     * オンラインアプリのレポートを出力する
     * </p>
     * 出力サンプル
     *
     * <pre>
     * ## ICEAXE-OCC-CONTRACT-T1
     *
     * | application    | Threads | tpm/thread | records/tx | succ | occ-try | occ-abort | occ-succ | occ abandoned retry | ltx-try | ltx-abort | ltx-succ |ltx abandoned retry|
     * |----------------|--------:|-----------:|-----------:|-----:|--------:|----------:|---------:|--------------------:|--------:|----------:|---------:|------------------:|
     * |Master Delete/Insert|1|-1|1|10137|10137|0|10137|0|0|0|0|0|
     * |Master Update|1|-1|1|3747|3747|0|3747|0|0|0|0|0|
     * |History Insert|1|-1|100|354|382|42|340|14|18|4|14|0|
     * |History Update|1|-1|1|798|986|207|779|19|19|0|19|0|
     * </pre>
     *
     * @param config
     * @param title
     * @param baselineTitle
     * @return
     */
    String createOnlineAppReport(Config config, String title, String baselineTitle) {
        StringBuilder sb = new StringBuilder();

        // タイトル
        sb.append("## " + title + "\n\n");

        // ヘッダ
        sb.append("| application    | Threads | tpm/thread | records/tx | succ | occ-try | occ-abort | occ-succ | occ<br>abandoned<br>retry | ltx-try | ltx-abort | ltx-succ |ltx<br>abandoned<br>retry|\n");
        sb.append("|----------------|--------:|-----------:|-----------:|-----:|--------:|----------:|---------:|--------------------------:|--------:|----------:|---------:|------------------------:|\n");


        // master delete+insert
        OnlineAppRecord masterDeleteInsert = new OnlineAppRecord();
        masterDeleteInsert.application = "Master Delete/Insert";
        masterDeleteInsert.threads = config.masterDeleteInsertThreadCount;
        masterDeleteInsert.tpmTthread = config.masterDeleteInsertRecordsPerMin;
        masterDeleteInsert.recordsTx = 1;
        masterDeleteInsert.setCounterValues(TxLabel.ONLINE_MASTER_INSERT, TxLabel.ONLINE_MASTER_DELETE);
        sb.append(masterDeleteInsert.toString());

        // master update
        OnlineAppRecord masterUpdate = new OnlineAppRecord();
        masterUpdate.application = "Master Update";
        masterUpdate.threads = config.masterUpdateThreadCount;
        masterUpdate.tpmTthread = config.masterUpdateRecordsPerMin;
        masterUpdate.recordsTx = 1;
        masterUpdate.setCounterValues(TxLabel.ONLINE_MASTER_UPDATE);
        sb.append(masterUpdate.toString());

        // history insert
        OnlineAppRecord historyInsert = new OnlineAppRecord();
        historyInsert.application = "History Insert";
        historyInsert.threads = config.historyInsertThreadCount;
        historyInsert.tpmTthread = config.historyInsertTransactionPerMin;
        historyInsert.recordsTx = config.historyInsertRecordsPerTransaction;
        historyInsert.setCounterValues(TxLabel.ONLINE_HISTORY_INSERT);
        sb.append(historyInsert.toString());

        // history update
        OnlineAppRecord historyUpdate = new OnlineAppRecord();
        historyUpdate.application = "History Update";
        historyUpdate.threads = config.historyUpdateThreadCount;
        historyUpdate.tpmTthread = config.historyUpdateRecordsPerMin;
        historyUpdate.recordsTx = 1;
        historyUpdate.setCounterValues(TxLabel.ONLINE_HISTORY_UPDATE);
        sb.append(historyUpdate.toString());


        // TxStatistic
        TxStatistics.saveTxStatisticsBundle(title);
        TxStatisticsBundle baseline = title.equals(baselineTitle) ? null
                : TxStatistics.getStatisticsBundle(baselineTitle);
        sb.append("\n\n");
        sb.append(TxStatistics.getReport(baseline));
        sb.append("\n");

        return sb.toString();
    }


    private int checkResult(Config config) {
        int n = 0;
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            Set<History> histories = manager.execute(TxOption.ofRTX(3, TxLabel.CHECK_RESULT), () -> {
                List<History> list =manager.getHistoryDao().getHistories();
                list.stream().forEach(h -> h.setSid(0)); // sidは比較対象でないので0をセット
                return new HashSet<>(list);
            });
            Set<Billing> billings = manager.execute(TxOption.ofRTX(3, TxLabel.CHECK_RESULT), () -> {
                List<Billing> list = manager.getBillingDao().getBillings();
                list.stream().forEach(b -> b.setBatchExecId(null)); // batchExecIdは比較対象でないのでnullをセット
                return new HashSet<>(list);
            });
            if (expectedHistories == null) {
                expectedHistories = histories;
            } else {
                n += checkSameSet(expectedHistories, histories);
            }
            if (expectedBillings == null) {
                expectedBillings = billings;
            } else {
                n += checkSameSet(expectedBillings, billings);
            }
        }
        return n;
    }

    /**
     * 二つのレコードを比較し差異のあるレコード数を返す
     *
     * @param <T>
     * @param expect
     * @param actual
     * @return
     */
    public static  <T> int checkSameSet(Set<T> expect, Set<T> actual) {
        int n = 0;
        for(T t: expect) {
            if (actual.contains(t)) {
                continue;
            }
            LOG.debug("only in expect:"  + t);
            n++;
        }
        for(T t: actual) {
            if (expect.contains(t)) {
                continue;
            }
            LOG.debug("only in actual:" + t);
            n++;
        }
        if (n != 0) {
            LOG.info("Did not get the same results.");
        }
        return n;
    }

    private static class Record {
        private static long baseElapsedMillis = -1;

        private TransactionOption option;
        private TransactionScope scope;
        private IsolationLevel isolationLevel;
        private int threadCount;
        private Instant start;
        private DbmsType dbmsType;
        private long elapsedMillis;
        private double elapsedRate;
        private int tryCount = 0;
        private int abortCount = 0;
        private Integer numberOfDiffrence = null;
        private long vsz = -1;
        private long rss = -1;
        private int onlineAppThreadCount;


        public static void resetBaseElapsedMillis() {
            baseElapsedMillis = -1;
        }

        public Record(Config config) {
            this.option = config.transactionOption;
            this.scope = config.transactionScope;
            this.isolationLevel = config.isolationLevel;
            this.threadCount = config.threadCount;
            this.dbmsType = config.dbmsType;
            this.onlineAppThreadCount = getOnlineAppThreadCount(config);
        }

        public void start() {
            LOG.info("Executing phoneBill.exec() with {}.", getParamString());
            start = Instant.now();
        }

        public void finish(Config config, int tryCount, int abortCount) {
            elapsedMillis = Duration.between(start, Instant.now()).toMillis();
            if (config.onlineOnly) {
                elapsedRate = -1d;
            } else {
                if (baseElapsedMillis == -1) {
                    elapsedRate = 1d;
                    baseElapsedMillis = elapsedMillis;
                } else {
                    elapsedRate = (double) elapsedMillis / baseElapsedMillis;
                }
            }
            TxStatistics.setDedicatedTimeMills(elapsedMillis);
            LOG.info("Finished phoneBill.exec(), elapsed secs = {}.", elapsedMillis / 1000.0);
            this.tryCount = tryCount;
            this.abortCount = abortCount;
        }

        public void setNumberOfDiffrence(int num) {
            numberOfDiffrence = num;
        }


        public void setMemInfo(long vsz, long rss) {
            this.vsz = vsz;
            this.rss = rss;
        }



        private String getParamString() {
            StringBuilder builder = new StringBuilder();
            builder.append("dbmsType=");
            builder.append(dbmsType);
            builder.append(", option=");
            builder.append(dbmsType.isTsurugi() ? option : isolationLevel);
            builder.append(", scope=");
            builder.append(scope);
            builder.append(", threadCount=");
            builder.append(threadCount);
            return builder.toString();
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(dbmsType);
            builder.append(",");
            builder.append(dbmsType.isTsurugi() ? option : isolationLevel);
            builder.append(",");
            builder.append(scope);
            builder.append(",");
            builder.append(threadCount);
            builder.append(",");
            builder.append(onlineAppThreadCount);
            builder.append(",");
            builder.append(String.format("%.3f", elapsedMillis / 1000.0));
            builder.append(",");
            if (elapsedRate < 0) {
                builder.append("---");
            } else {
                builder.append(String.format("%.2f%%", elapsedRate * 100));
            }
            builder.append(",");
            builder.append(tryCount);
            builder.append(",");
            builder.append(abortCount);
            builder.append(",");
            builder.append(numberOfDiffrence == null ? "---" : numberOfDiffrence);
            builder.append(",");
            builder.append(vsz == -1 ? "-" : String.format("%.1f", vsz / 1024f / 1024f / 1024f));
            builder.append(",");
            builder.append(rss == -1 ? "-" : String.format("%.1f", rss / 1024f / 1024f / 1024f));
            return builder.toString();
        }

        public static String header() {
            return "dbmsType, option, scope, batchThreads, onlineAppThreads, elapsedSeconds, elapsedRate, tryCount, abortCount, diffrence, vsz(GB), rss(GB)";
        }
    }


    private static class OnlineAppRecord {
        String application;
        int threads;
        int tpmTthread;
        int recordsTx;
        int succ = 0;
        int occTry = 0;
        int occAbort = 0;
        int occSucc = 0;
        int occAbandonedRtry = 0;
        int ltxTry = 0;
        int ltxAbort = 0;
        int ltxSucc = 0;
        int ltxAbandonedRtry = 0;

        void setCounterValues(TxLabel ...txLabels) {
            for (TxLabel txLabel : txLabels) {
                occTry += PhoneBillDbManager.getCounter(CounterKey.of(txLabel, CounterName.OCC_TRY));
                occAbort += PhoneBillDbManager.getCounter(CounterKey.of(txLabel, CounterName.OCC_ABORT));
                occSucc += PhoneBillDbManager.getCounter(CounterKey.of(txLabel, CounterName.OCC_SUCC));
                occAbandonedRtry += PhoneBillDbManager.getCounter(CounterKey.of(txLabel, CounterName.OCC_ABANDONED_RETRY));
                ltxTry += PhoneBillDbManager.getCounter(CounterKey.of(txLabel, CounterName.LTX_TRY));
                ltxAbort += PhoneBillDbManager.getCounter(CounterKey.of(txLabel, CounterName.LTX_ABORT));
                ltxSucc += PhoneBillDbManager.getCounter(CounterKey.of(txLabel, CounterName.LTX_SUCC));
                ltxAbandonedRtry += PhoneBillDbManager.getCounter(CounterKey.of(txLabel, CounterName.LTX_ABANDONED_RETRY));
                succ += occSucc + ltxSucc;
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("|");
            sb.append(application);
            sb.append("|");
            sb.append(threads);
            sb.append("|");
            sb.append(tpmTthread);
            sb.append("|");
            sb.append(recordsTx);
            sb.append("|");
            sb.append(succ);
            sb.append("|");
            sb.append(occTry);
            sb.append("|");
            sb.append(occAbort);
            sb.append("|");
            sb.append(occSucc);
            sb.append("|");
            sb.append(occAbandonedRtry);
            sb.append("|");
            sb.append(ltxTry);
            sb.append("|");
            sb.append(ltxAbort);
            sb.append("|");
            sb.append(ltxSucc);
            sb.append("|");
            sb.append(ltxAbandonedRtry);
            sb.append("|");
            sb.append("\n");
            return sb.toString();
        }
    }
}
