package com.tsurugidb.benchmark.phonebill.app;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.billing.PhoneBill;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.testdata.CreateTestData;

/**
 * 以下の条件を変えて、バッチの処理時間がどう変化するのかを測定する
 * <ul>
 *   <li> オンラインアプリケーションを動かさない場合
 *   <li> 各オンラインアプリケーションを単独で動かした場合
 *   <li> すべてのオンラインアプリケーションを動かした場合
 * </ul>
 * 上記以外の値はConfigで指定された値を使用する
 *
 */
public class OnlineAppBench extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(OnlineAppBench.class);
    private PhoneBillDbManager manager;


    public static void main(String[] args) throws Exception {
        OnlineAppBench threadBench = new OnlineAppBench();
        Config config = Config.getConfig(args);
        CrashDumper.enable();
        threadBench.execute(config);
        CrashDumper.disable();
    }


    @Override
    public void execute(Config config) throws Exception {
        manager = PhoneBillDbManager.createPhoneBillDbManager(config);
        int historyInsertTransactionPerMin = config.historyInsertTransactionPerMin;
        int historyUpdateRecordsPerMin = config.historyUpdateRecordsPerMin;
        int masterInsertReccrdsPerMin = config.masterDeleteInsertRecordsPerMin;
        int masterUpdateRecordsPerMin = config.masterUpdateRecordsPerMin;
        long elapsedTime;

        // オンラインアプリケーションを動かさない場合
        config.historyInsertTransactionPerMin = 0;
        config.historyUpdateRecordsPerMin = 0;
        config.masterDeleteInsertRecordsPerMin = 0;
        config.masterUpdateRecordsPerMin = 0;
        elapsedTime = execBatch(config);
        LOG.info("No online application, elapsed time = {} ms", elapsedTime);

        // 各オンラインアプリケーションを単独で動かした場合
        config.historyInsertTransactionPerMin = historyInsertTransactionPerMin;
        config.historyUpdateRecordsPerMin = 0;
        config.masterDeleteInsertRecordsPerMin = 0;
        config.masterUpdateRecordsPerMin = 0;
        elapsedTime = execBatch(config);
        LOG.info("History insert online application, elapsed time = {} ms", elapsedTime);

        config.historyInsertTransactionPerMin = 0;
        config.historyUpdateRecordsPerMin = historyUpdateRecordsPerMin;
        config.masterDeleteInsertRecordsPerMin = 0;
        config.masterUpdateRecordsPerMin = 0;
        elapsedTime = execBatch(config);
        LOG.info("History update online application, elapsed time = {} ms", elapsedTime);

        config.historyInsertTransactionPerMin = 0;
        config.historyUpdateRecordsPerMin = 0;
        config.masterDeleteInsertRecordsPerMin = masterInsertReccrdsPerMin;
        config.masterUpdateRecordsPerMin = 0;
        elapsedTime = execBatch(config);
        LOG.info("Master insert online application, elapsed time = {} ms", elapsedTime);

        config.historyInsertTransactionPerMin = 0;
        config.historyUpdateRecordsPerMin = 0;
        config.masterDeleteInsertRecordsPerMin = 0;
        config.masterUpdateRecordsPerMin = masterUpdateRecordsPerMin;
        elapsedTime = execBatch(config);
        LOG.info("Master update online application, elapsed time = {} ms", elapsedTime);

        // すべてのオンラインアプリケーションを動かした場合
        config.historyInsertTransactionPerMin = historyInsertTransactionPerMin;
        config.historyUpdateRecordsPerMin = historyUpdateRecordsPerMin;
        config.masterDeleteInsertRecordsPerMin = historyUpdateRecordsPerMin;
        config.masterUpdateRecordsPerMin = masterUpdateRecordsPerMin;
        elapsedTime = execBatch(config);
        LOG.info("All online application, elapsed time = {} ms", elapsedTime);
    }


    /**
     * 指定のconfigでバッチを実行し、処理時間を返す
     *
     * @param config
     * @return
     * @throws Exception
     */
    private long execBatch(Config config) throws Exception {
        PhoneBill phoneBill = new PhoneBill();
        new CreateTable().execute(config);
        new CreateTestData().execute(config);
        beforeExec(config);
        phoneBill.execute(config);
        afterExec(config);
        return phoneBill.getElapsedTime();
    }


    private void afterExec(Config config) {
        List<History> histories = new ArrayList<>();
        manager.execute(TxOption.ofRTX(3, TxLabel.CHECK_RESULT), () -> {
            histories.addAll(manager.getHistoryDao().getHistories());
        });
        int historyUpdated = countUpdated(histories, orgHistories, History::getKey);
        int historyInserted = histories.size() - orgHistories.size();

        List<Contract> contracts = new ArrayList<>();
        manager.execute(TxOption.ofRTX(3, TxLabel.CHECK_RESULT), () -> {
            contracts.addAll(manager.getContractDao().getContracts());
        });
        int masterUpdated = countUpdated(contracts, orgContracts, Contract::getKey);
        int masterInserted = contracts.size() - orgContracts.size();
        contracts.clear();
        orgContracts.clear();

        LOG.info("history updated = " + historyUpdated);
        LOG.info("history inserted = " + historyInserted);
        LOG.info("master updated = " + masterUpdated);
        LOG.info("master inserted = " + masterInserted);
    }


    /**
     * EntityのList newList と orgListを比較して更新されたレコード数を返す
     *
     * @param historyMap
     * @return
     */
    private <K,T> int countUpdated(List<T> newList, List<T> orgList, Function<T, K> getKeyFunc) {
        Map<K, T> map = newList.stream().collect(Collectors.toMap(t -> getKeyFunc.apply(t), t -> t));
        int c = 0;
        for (T t: orgList) {
            T nh = map.get(getKeyFunc.apply(t));
            if (!t.equals(nh)) {
                c++;
            }
        }
        return c;
    }


    List<History> orgHistories;
    List<Contract> orgContracts;

    private void beforeExec(Config config) {
        manager.execute(TxOption.ofRTX(3, TxLabel.INITIALIZE), () -> {
            orgHistories = manager.getHistoryDao().getHistories();
            orgContracts = manager.getContractDao().getContracts();
        });
    }
}
