package com.tsurugidb.benchmark.phonebill.testdata;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.testdata.GenerateHistoryTask.Result;
import com.tsurugidb.benchmark.phonebill.testdata.TestDataGenerator.HistoryWriter;

/**
 * 履歴データを作成するタスク
 *
 */
public class GenerateHistoryTask implements Callable<Result> {
    private static final Logger LOG = LoggerFactory.getLogger(GenerateHistoryTask.class);

    /*
     * Task ID
     */
    private int taskId;

    /**
     * 同一のキーのデータを作らないために作成済みのHistoryDataのKeyを記録するSet
     */
    private Set<HistoryKey> keySet;

    /**
     * 乱数生成器
     */
    private Random random;


    /**
     * 発信者電話番号のSelector
     */
    private PhoneNumberSelector callerPhoneNumberSelector;

    /**
     * 受信者電話番号のSelector
     */
    private PhoneNumberSelector recipientPhoneNumberSelector;

    /**
     * 通話時間生成器
     */
    private CallTimeGenerator callTimeGenerator;

    /**
     * 電話番号生成器
     */
    private PhoneNumberGenerator phoneNumberGenerator;

    /**
     * 通話開始時刻の最小値
     */
    private long start;

    /**
     * 通話開始時刻の最大値+1
     */
    private long end;


    /**
     * 生成する通話履歴数
     */
    private long numbeOfHistory;


    /**
     * 通話履歴の出力先
     */
    private HistoryWriter historyWriter;


    /**
     * 契約マスタの情報
     */
    private ContractInfoReader contractInfoReader;

    /**
     * コンフィグ
     */
    private Config config;

    /**
     * コンストラクタ.
     * <br>
     *
     * @param config
     * @param random
     * @param contractInfoReader
     * @param phoneNumberGenerator
     * @param durationList
     * @param start
     * @param end
     * @param writeSize
     * @param n
     * @throws IOException
     */
    public GenerateHistoryTask(Params params)  {
        random = params.random;
        phoneNumberGenerator = params.phoneNumberGenerator;
        start = params.start;
        end = params.end;
        numbeOfHistory = params.numberOfHistory;
        historyWriter=params.historyWriter;
        config = params.config;
        try {
            contractInfoReader = ContractInfoReader.create(config, params.accessor, params.random);
            contractInfoReader.loadActiveBlockNumberList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        taskId = params.taskId;
    }

    @Override
    public Result call() throws Exception {
        Result result = new Result(taskId);
        LOG.debug("start task id = " + taskId);
        try {
            init();
            for (long i = 0; i < numbeOfHistory; i++) {
                History h = createHistoryRecord();
                historyWriter.write(h);
            }
        } catch (RuntimeException e) {
            result.success = false;
            result.e = e;
            LOG.debug("end task id =" + taskId + " with error", e);
        } finally {
            historyWriter.cleanup();
        }
        LOG.debug("end task id = " + taskId);
        return result;
    }



    public void init() throws IOException {
        historyWriter.init();
        keySet = new HashSet<HistoryKey>((int)numbeOfHistory);
        callerPhoneNumberSelector = PhoneNumberSelector.createSelector(random,
                config.callerPhoneNumberDistribution,
                config.callerPhoneNumberScale,
                config.callerPhoneNumberShape, contractInfoReader);
        recipientPhoneNumberSelector = PhoneNumberSelector.createSelector(random,
                config.recipientPhoneNumberDistribution,
                config.recipientPhoneNumberScale,
                config.recipientPhoneNumberShape, contractInfoReader);
        callTimeGenerator = CallTimeGenerator.createCallTimeGenerator(random, config);
    }

    /**
     * 通話履歴を生成する
     *
     * @param targetDuration
     * @return
     */
    private History createHistoryRecord() {
        // 重複しないキーを選ぶ
        HistoryKey key = new HistoryKey();
        int counter = 0;
        for (;;) {
            key.startTime = TestDataUtils.getRandomLong(random, start, end);
            key.callerPhoneNumber = callerPhoneNumberSelector.selectPhoneNumber(key.startTime, -1);
            if (keySet.contains(key)) {
                if (++counter > 5) {
                    LOG.info("A duplicate key was found, so another key will be created.(key = {}, KeySetSize = {} ", key, keySet.size());
                    counter = 0;
                }
            } else {
                keySet.add(key);
                return createHistoryRecord(key);
            }
        }
    }

    /**
     * 指定のキーを持つ通話履歴を生成する
     *
     * @param key
     * @return
     */
    public History createHistoryRecord(HistoryKey key) {
        History history = new History();
        history.setStartTime(new Timestamp(key.startTime));

        // 電話番号の生成
        long r = recipientPhoneNumberSelector.selectPhoneNumber(key.startTime, key.callerPhoneNumber);
        history.setCallerPhoneNumber(phoneNumberGenerator.getPhoneNumber(key.callerPhoneNumber));
        history.setRecipientPhoneNumber(phoneNumberGenerator.getPhoneNumber(r));

        // 料金区分(発信者負担、受信社負担)
        history.setPaymentCategorty(random.nextInt(2) == 0 ? "C" : "R");

        // 通話時間
        history.setTimeSecs(callTimeGenerator.getTimeSecs());
        return history;
    }

    /**
     * 指定の通話開始時刻のキーを作成する
     *
     * @param startTime
     * @return
     */
    public HistoryKey createkey(long startTime) {
        HistoryKey key = new HistoryKey();
        key.startTime = startTime;
        key.callerPhoneNumber = callerPhoneNumberSelector.selectPhoneNumber(startTime, -1);
        return key;
    }


    /**
     * アクティブな契約マスタのブロック情報をリロードする
     * @throws IOException
     */
    public void reloadActiveBlockNumberList() throws IOException {
        contractInfoReader.loadActiveBlockNumberList();
    }


    /**
     * タスクのパラメータ
     *
     */
    public static class Params implements Cloneable {
        int taskId;
        Config config;
        Random random;
        ContractBlockInfoAccessor accessor;
        PhoneNumberGenerator phoneNumberGenerator;
        long start;
        long end;
        long numberOfHistory;
        public HistoryWriter historyWriter;

        @Override
        public Params clone() {
            try {
                return (Params) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new InternalError(e.toString());
            }
        }
    }

    /**
     * タスクの実行結果
     *
     */
    public static class Result {
        int taskId;
        boolean success = true;
        Exception e = null;

        public Result(int taskId) {
            this.taskId = taskId;
        }
    }

    /**
     * contractInfoReaderを返す(UT用)
     *
     * @return contractInfoReader
     */
    public ContractInfoReader getContractInfoReader() {
        return contractInfoReader;
    }
}
