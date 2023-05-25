package com.tsurugidb.benchmark.phonebill.online;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.ExecutableCommand;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.CreateTestData;
import com.tsurugidb.benchmark.phonebill.testdata.DbContractBlockInfoInitializer;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 *
 * バッチを動かさないでオンラインアプリを動かす.
 * <br>
 * 以下の条件で実行し、最後に結果サマリを表示する
 * <br>
 * 1. スレッド数 1, 2, 4, 8, 10, 12<br>
 * 2 各オンラインアプリを単独で動かした場合と、4つのオンラインアプリを同時に動かしたとき<br>
 * 3.各オンラインアプリは連続実行する<br>
 *
 */
public class RunOnlineApp extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(RunOnlineApp.class);

    public static void main(String[] args) throws Exception {
        Config config = Config.getConfig(args);
        RunOnlineApp runOnlineApp = new RunOnlineApp();
        runOnlineApp.execute(config);
    }

    @Override
    public void execute(Config config) throws Exception {
        int[] threadCounts = {1, 2, 4, 8, 16, 32, 64, 128, 256};
        for (int threadCount: threadCounts ) {
            config.masterDeleteInsertRecordsPerMin = -1;
            config.masterUpdateRecordsPerMin = -1;
            config.historyInsertRecordsPerTransaction = 10000;
            config.historyInsertTransactionPerMin = -1;
            config.historyUpdateRecordsPerMin = -1;

            config.masterDeleteInsertThreadCount = threadCount;
            config.masterUpdateThreadCount = 0;
            config.historyInsertThreadCount = 0;
            config.historyUpdateThreadCount = 0;
            executeOnlineApps(config);

            config.masterDeleteInsertThreadCount = 0;
            config.masterUpdateThreadCount = threadCount;
            config.historyInsertThreadCount = 0;
            config.historyUpdateThreadCount = 0;
            executeOnlineApps(config);

            config.masterDeleteInsertThreadCount = 0;
            config.masterUpdateThreadCount = 0;
            config.historyInsertThreadCount = threadCount;
            config.historyUpdateThreadCount = 0;
            executeOnlineApps(config);

            config.masterDeleteInsertThreadCount = 0;
            config.masterUpdateThreadCount = 0;
            config.historyInsertThreadCount = 0;
            config.historyUpdateThreadCount = threadCount;
            executeOnlineApps(config);

            if (threadCount <= 64) {
                config.masterDeleteInsertThreadCount = threadCount;
                config.masterUpdateThreadCount = threadCount;
                config.historyInsertThreadCount = threadCount;
                config.historyUpdateThreadCount = threadCount;
                executeOnlineApps(config);
            }
        }
    }

    @SuppressFBWarnings(value={"DMI_RANDOM_USED_ONLY_ONCE"})
    private Result executeOnlineApps(Config config) throws Exception {
        // テストデータの初期化
        new CreateTestData().execute(config);
        DbContractBlockInfoInitializer initializer = new DbContractBlockInfoInitializer(config);
        ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager(initializer);

        Random random = new Random();
        List<AbstractOnlineApp> MasterDeleteInsertApps = new ArrayList<>();
        List<AbstractOnlineApp> masterUpdateApps = new ArrayList<>();
        List<AbstractOnlineApp> historyInsertApps = new ArrayList<>();
        List<AbstractOnlineApp> historyUpdateApps = new ArrayList<>();

        // KeySelectorの初期化
        RandomKeySelector<Contract.Key> keySelector;
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            List<Key> keys = manager.execute(TxOption.of(), () -> {
                return manager.getContractDao().getAllPrimaryKeys();
            });
            keySelector = new RandomKeySelector<>(keys, random, config.onlineAppRandomAtLeastOnceRate,
                    config.onlineAppRandomCoverRate);        }

        // MasterDeleteInsertAppの初期化
        for (int i = 0; i < config.masterDeleteInsertThreadCount; i++) {
            AbstractOnlineApp task = new MasterDeleteInsertApp(config, new Random(random.nextInt()), keySelector);
            task.setName(i);
            MasterDeleteInsertApps.add(task);
        }

        // MasterUpdateAppの初期化
        for (int i = 0; i < config.masterUpdateThreadCount; i++) {
            AbstractOnlineApp task = new MasterUpdateApp(config, new Random(random.nextInt()), keySelector);
            task.setName(i);
            masterUpdateApps.add(task);
        }

        // historyInsertAppの初期化
        historyInsertApps
                .addAll(HistoryInsertApp.createHistoryInsertApps(config, new Random(random.nextInt()), accessor,
                        config.historyInsertThreadCount));

        // HistoryUpdateAppの初期化
        for (int i = 0; i < config.historyUpdateThreadCount; i++) {
            AbstractOnlineApp task = new HistoryUpdateApp(config, new Random(random.nextInt()), keySelector);
            task.setName(i);
            historyUpdateApps.add(task);
        }

        // オンラインアプリの実行
        List<AbstractOnlineApp> list = new ArrayList<AbstractOnlineApp>();
        list.addAll(MasterDeleteInsertApps);
        list.addAll(masterUpdateApps);
        list.addAll(historyInsertApps);
        list.addAll(historyUpdateApps);
        final ExecutorService service = Executors.newFixedThreadPool(list.size());
        try {
            // オンラインアプリを1分間実行する
            list.parallelStream().forEach(task -> service.submit(task));
            Thread.sleep(60 * 1000);
        } finally {
            // オンラインアプリを終了する
            list.stream().forEach(task -> task.terminate());
            service.shutdown();
            service.awaitTermination(5, TimeUnit.MINUTES);
        }
        // 実行結果の集計
        Result result = new Result();
        result.masterInsertThreadCount = config.masterDeleteInsertThreadCount;
        result.masterUpdateThreadCount = config.masterUpdateThreadCount;
        result.historyInsertThreadCount = config.historyInsertThreadCount;
        result.historyUpdateThreadCount = config.historyUpdateThreadCount;

        result.masterInsertExecCount = MasterDeleteInsertApps.stream().mapToInt(app -> app.getExecCount()).sum();
        result.masterUpdateExecCount = masterUpdateApps.stream().mapToInt(app -> app.getExecCount()).sum();
        result.historyInsertExecCount = historyInsertApps.stream().mapToInt(app -> app.getExecCount()).sum();
        result.historyUpdateExecCount = historyUpdateApps.stream().mapToInt(app -> app.getExecCount()).sum();

        result.masterInsertRetryCount = MasterDeleteInsertApps.stream().mapToInt(app -> app.getRetryCount()).sum();
        result.masterUpdateRetryCount = masterUpdateApps.stream().mapToInt(app -> app.getRetryCount()).sum();
        result.historyInsertRetryCount = historyInsertApps.stream().mapToInt(app -> app.getRetryCount()).sum();
        result.historyUpdateRetryCount = historyUpdateApps.stream().mapToInt(app -> app.getRetryCount()).sum();
        LOG.info(result.toString());
        return result;
    }

    static class Result {
        int masterUpdateThreadCount;
        int masterInsertThreadCount;
        int historyUpdateThreadCount;
        int historyInsertThreadCount;
        int masterUpdateExecCount;
        int masterInsertExecCount;
        int historyUpdateExecCount;
        int historyInsertExecCount;
        int masterUpdateRetryCount;
        int masterInsertRetryCount;
        int historyUpdateRetryCount;
        int historyInsertRetryCount;

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("Result [masterUpdateThreadCount=");
            builder.append(masterUpdateThreadCount);
            builder.append(", masterDeleteInsertThreadCount=");
            builder.append(masterInsertThreadCount);
            builder.append(", historyUpdateThreadCount=");
            builder.append(historyUpdateThreadCount);
            builder.append(", historyInsertThreadCount=");
            builder.append(historyInsertThreadCount);
            builder.append(", masterUpdateExecCount=");
            builder.append(masterUpdateExecCount);
            builder.append(", masterInsertExecCount=");
            builder.append(masterInsertExecCount);
            builder.append(", historyUpdateExecCount=");
            builder.append(historyUpdateExecCount);
            builder.append(", historyInsertExecCount=");
            builder.append(historyInsertExecCount);
            builder.append(", masterUpdateRetryCount=");
            builder.append(masterUpdateRetryCount);
            builder.append(", masterInsertRetryCount=");
            builder.append(masterInsertRetryCount);
            builder.append(", historyUpdateRetryCount=");
            builder.append(historyUpdateRetryCount);
            builder.append(", historyInsertRetryCount=");
            builder.append(historyInsertRetryCount);
            builder.append("]");
            return builder.toString();
        }
    }
}
