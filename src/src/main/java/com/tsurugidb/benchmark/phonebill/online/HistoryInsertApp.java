package com.tsurugidb.benchmark.phonebill.online;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.TxOption.Table;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.ContractInfoReader;
import com.tsurugidb.benchmark.phonebill.testdata.GenerateHistoryTask;
import com.tsurugidb.benchmark.phonebill.testdata.HistoryKey;
import com.tsurugidb.benchmark.phonebill.testdata.TestDataGenerator;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

/**
 * 通話履歴を追加するオンラインアプリケーション.
 * <br>
 * 話開始時刻が、baseTime ～ baseTime + durationの通話履歴を生成する。atScheduleListCreated()が
 * 呼び出されるたびに、baseTimeをCREATE_SCHEDULE_INTERVAL_MILLSだけシフトする。
 *
 */
public class HistoryInsertApp extends AbstractOnlineApp {
    private static final Logger LOG = LoggerFactory.getLogger(HistoryInsertApp.class);
    private int historyInsertRecordsPerTransaction;
    private GenerateHistoryTask generateHistoryTask;
    private long baseTime;
    private int duration;
    private List<History> histories = new ArrayList<>();
    private Random random;

    /**
     * // 同一のPKのレコードを生成しないためにPK値を記録するためのセット
     */
    private Set<HistoryKey> keySet = new HashSet<HistoryKey>();

    /**
     * コンストラクタ
     *
     * @param contractInfoReader
     * @param config
     * @param seed
     * @param baseTime
     * @param duration
     * @throws IOException
     */
    private HistoryInsertApp(ContractBlockInfoAccessor accessor, Config config, Random random, long baseTime,
            int duration) throws IOException {
        super(config.historyInsertTransactionPerMin, config, random);
        this.historyInsertRecordsPerTransaction = config.historyInsertRecordsPerTransaction;
        this.baseTime = baseTime;
        this.duration = duration;
        this.random = random;
        TestDataGenerator testDataGenerator = new TestDataGenerator(config, random, accessor);
        generateHistoryTask = testDataGenerator.getGenerateHistoryTaskForOnlineApp();
    }

    /**
     * 指定した数だけ、HistoryInsertAppのインスタンスを作成し、リストで返す.
     * <br>
     * 複数のHistoryInsertAppを同時に動かしても、キーの重複が起きないように、baseTimeとdurationの値を調整する。
     *
     * @param contractInfoReader
     * @param config
     * @param seed
     * @param num
     * @return
     * @throws IOException
     */
    public static List<AbstractOnlineApp> createHistoryInsertApps(Config config, Random random,
            ContractBlockInfoAccessor accessor, int num) throws IOException {
        List<AbstractOnlineApp> list = new ArrayList<>();
        if (num > 0) {
            int duration = CREATE_SCHEDULE_INTERVAL_MILLS / num;
            long baseTime = getBaseTime(config);
            for (int i = 0; i < num; i++) {
                if (i != 0) {
                    random = new Random(random.nextInt());
                }
                AbstractOnlineApp app = new HistoryInsertApp(accessor, config, random, baseTime, duration);
                app.setName(i);
                baseTime += duration;
                list.add(app);
            }
        }
        return list;
    }


    @Override
    protected void atScheduleListCreated(List<Long> scheduleList) throws IOException {
        // スケジュールに合わせてbaseTimeをシフトする
        baseTime = getBaseTime() + CREATE_SCHEDULE_INTERVAL_MILLS;
        // baseTimeのシフトにより、これ以前のキーとキーが重複することはないので、keySetをクリアする
        keySet.clear();
        // スケジュール作成時に、契約マスタのブロック情報をアップデートする
        generateHistoryTask.reloadActiveBlockNumberList();
    }

    /**
     * baseTimeをセットする。履歴データの通話開始時刻は初期データの通話開始時刻の最後の日の翌日0時にする。
     * ただし、既にhistoryInsertAppによるデータが存在する場合は、存在する時刻の最大値を指定する。
     *
     * @param config
     * @return
     */
    static long getBaseTime(Config config) {
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            HistoryDao dao = manager.getHistoryDao();
            long maxStartTime = manager.execute(TxOption.ofRTX(3, TxLabel.INITIALIZE),
                    () -> dao.getMaxStartTime());
            return Math.max(maxStartTime, DateUtils.nextDate(config.historyMaxDate).getTime());
        }
    }


    @Override
    protected void createData(ContractDao contractDao, HistoryDao historyDao) {
        histories.clear();
        for (int i = 0; i < historyInsertRecordsPerTransaction; i++) {
            HistoryKey key;
            do {
                long startTime = getBaseTime() + random.nextInt(duration);
                key = generateHistoryTask.createkey(startTime);
            } while (keySet.contains(key));
            keySet.add(key);
            histories.add(generateHistoryTask.createHistoryRecord(key));
        }
    }

    @Override
    protected void updateDatabase(ContractDao contractDao, HistoryDao historyDao) {
        historyDao.batchInsert(histories);
        LOG.debug("ONLINE_APP: Insert {} records to history.", historyInsertRecordsPerTransaction);
    }

    /**
     * baseTimeを返す(UT用)
     *
     * @return
     */
    long getBaseTime() {
        return baseTime;
    }

    /**
     * keySetを返す(UT用)
     *
     * @return
     */
    Set<HistoryKey> getKeySet() {
        return keySet;
    }

    /**
     * ContractInfoReaderを返す(UT用)
     */
    ContractInfoReader getContractInfoReader() {
        return generateHistoryTask.getContractInfoReader();
    }


    @Override
    public TxLabel getTxLabel() {
        return TxLabel.ONLINE_HISTORY_INSERT;
    }

    @Override
    public Table getWritePreserveTable() {
        return Table.HISTORY;
    }

    @Override
    protected void afterCommitSuccess() {
        // Nothing to do
    }
}
