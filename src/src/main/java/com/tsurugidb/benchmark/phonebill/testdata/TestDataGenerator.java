package com.tsurugidb.benchmark.phonebill.testdata;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.jdbc.Duration;
import com.tsurugidb.benchmark.phonebill.testdata.GenerateHistoryTask.Params;
import com.tsurugidb.benchmark.phonebill.testdata.GenerateHistoryTask.Result;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class TestDataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(TestDataGenerator.class);

    /**
     * 統計情報
     */
    private Statistics statistics;

    /**
     * trueのときにDBに書き込まずデータを生成せず、統計情報を出力する
     */
    private boolean statisticsOnly;


    /**
     * コンフィグレーション
     */
    private Config config;

    /**
     * 電話番号生成器
     */
    private PhoneNumberGenerator phoneNumberGenerator;


    /**
     * 一度にインサートする行数
     */
    private static final int SQL_BATCH_EXEC_SIZE = 300000;

    /**
     * 乱数生成器
     */
    private Random random;

    /**
     * 契約マスタの情報
     */
    private ContractInfoReader contractInfoReader;


    /**
     * 契約のブロックに関する情報にアクセスするためのアクセサ
     */
    private ContractBlockInfoAccessor accessor;

    /**
     * 乱数のシードとaccessorを指定可能なコンストラクタ。accessorにnullが
     * 指定された場合は、デフォルトのSingleProcessContractBlockManagerを使用する。
     *
     * @param config
     * @param seed
     * @param accessor
     * @throws IOException
     */
    public TestDataGenerator(Config config, Random random, ContractBlockInfoAccessor accessor) throws IOException {
        this.config = config;
        if (config.minDate.getTime() >= config.maxDate.getTime()) {
            throw new RuntimeException("maxDate is less than or equal to minDate, minDate =" + config.minDate + ", maxDate = "
                    + config.maxDate);
        }
        this.random = random;
        phoneNumberGenerator = new PhoneNumberGenerator(config);
        this.accessor = accessor;
        this.contractInfoReader = ContractInfoReader.create(config, accessor, random);
        statistics = new Statistics(config.historyMinDate, config.historyMaxDate);
    }


    /**
     * オンラインアプリ用のGenerateHistoryTaskを生成する
     *
     * @return
     * @throws IOException
     */
    public GenerateHistoryTask getGenerateHistoryTaskForOnlineApp() throws IOException {
        Params params = createTaskParams(0, 0, 0,1).get(0);
        params.historyWriter = new DummyHistoryWriter();
        GenerateHistoryTask generateHistoryTask = new GenerateHistoryTask(params);
        generateHistoryTask.init();
        return generateHistoryTask;
    }

    /**
     * @param start 通話開始時刻の最小値
     * @param end 通話開始時刻の最大値 + 1
     * @param numbeOfHistory 作成する履歴数
     * @param numberOfTasks 作成するタスク数
     * @return
     */
    private List<Params> createTaskParams(long start, long end, long numbeOfHistory,
            int numberOfTasks) {
        Params params = new Params();
        params.taskId = 0;
        params.config = config;
        params.random = new Random(random.nextLong());
        params.accessor = accessor;
        params.phoneNumberGenerator = phoneNumberGenerator;
        params.start = start;
        params.end = end;
        params.numberOfHistory = numbeOfHistory;

        if (numberOfTasks <= 1) {
            return Collections.singletonList(params);
        } else {
            return createParams(params, numberOfTasks);
        }
    }

    /**
     * 指定のパラメータを元に指定の数にタスクを分割したパラメータを生成する
     *
     * @param params
     * @param numberOfTasks
     * @return
     */
    private List<Params> createParams(Params params, int numberOfTasks) {
        List<Params> list = new ArrayList<>(numberOfTasks);

        long firstNumberOfHistory = 0;
        for(int i = 0; i < numberOfTasks; i++) {
            Params dividedParams = params.clone();
            dividedParams.taskId = i;
            if (i == 0) {
                dividedParams.start = config.historyMinDate.getTime();
                // dividedParams.numbeOfHistory = params.numbeOfHistory / numberOfTasks で計算すると端数がでるので、
                // i == 0 のときに端数を調整した値を入れる
                firstNumberOfHistory = config.numberOfHistoryRecords
                        - ((long)config.maxNumberOfLinesHistoryCsv) * (numberOfTasks - 1);
                dividedParams.numberOfHistory = firstNumberOfHistory;
            } else {
                dividedParams.numberOfHistory = config.maxNumberOfLinesHistoryCsv;
                // 各タスクに異なる乱数発生器を使用する
                dividedParams.random = new Random(random.nextLong());
                dividedParams.start = list.get(i - 1).end;
            }
            double scale = ((double)firstNumberOfHistory + (double)(config.maxNumberOfLinesHistoryCsv) * i)
            / (double)(config.numberOfHistoryRecords);
            dividedParams.end = params.start + Math.round((params.end - params.start) * scale);
            list.add(dividedParams);
        }
        return list;
    }

        /**
     * 二つの期間に共通の期間を返す
     *
     * @param d1
     * @param d2
     * @return 共通な期間、共通な期間がない場合nullを返す。
     */
    public static Duration getCommonDuration(Duration d1, Duration d2) {
        // d1, d2に共通な期間がない場合
        if (d1.end < d2.start) {
            return null;
        }
        if (d2.end < d1.start) {
            return null;
        }
        if (d1.start < d2.start) {
            if (d1.end < d2.end) {
                return new Duration(d2.start, d1.end);
            } else {
                return d2;
            }
        } else {
            if (d1.end < d2.end) {
                return d1;
            } else {
                return new Duration(d1.start, d2.end);
            }
        }
    }

    /**
     * 契約マスタのテストデータをDBに生成する
     *
     * @param manager
     */
    public void generateContractsToDb(PhoneBillDbManager manager) {
        int batchSize = 0;
        List<Contract> contracts = new ArrayList<>(SQL_BATCH_EXEC_SIZE);
        for (long n = 0; n < config.numberOfContractsRecords; n++) {
            Contract c = getNewContract().clone();
            contracts.add(c);
            if (++batchSize == SQL_BATCH_EXEC_SIZE) {
                insertContracts(manager, contracts);
            }
        }
        insertContracts(manager, contracts);
    }

    /**
     * DAOを使用してContractテーブルにデータを入れる
     *
     * @param tm
     * @param manager
     * @param contracts
     */
    private void insertContracts(PhoneBillDbManager manager, List<Contract> contracts) {
        ContractDao dao = manager.getContractDao();
        manager.execute(TxOption.ofOCC(Integer.MAX_VALUE, TxLabel.TEST_DATA_GENERATOR), () -> dao.batchInsert(contracts));
        contracts.clear();
    }

    /**
     * @return
     * @throws IOException
     */
    public Contract getNewContract()  {
        try {
            return  contractInfoReader.getNewContract();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 契約マスタのテストデータのCSVファイルを生成する
     *
     * @throws IOException
     */
    public void generateContractsToCsv(Path path) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedWriter bw = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);) {
            for (long n = 0; n < config.numberOfContractsRecords; n++) {
                Contract c = getNewContract();
                sb.setLength(0);
                sb.append(c.getPhoneNumber());
                sb.append(',');
                sb.append(c.getStartDate());
                sb.append(',');
                if (c.getEndDate() != null) {
                    sb.append(c.getEndDate());
                }
                sb.append(',');
                sb.append(c.getRule());
                bw.write(sb.toString());
                bw.newLine();
            }
        }
    }

    /**
     * 通話履歴のテストデータをDBに作成する
     *
     * @throws IOException
     */
    public void generateHistoryToDb(Config config) {
        List<Params> paramsList = createParamsList(10000);
        for(Params params: paramsList) {
            params.historyWriter = new DaoHistoryWriter(config);
        }
        generateHistory(paramsList);
    }


    /**
     * 通話履歴のテストデータをCSVファイルに作成する
     *
     * @throws IOException
     */
    public void generateHistoryToCsv(Path dir)  {
        List<Params> paramsList = createParamsList(config.maxNumberOfLinesHistoryCsv);
        for(Params params: paramsList) {
            Path outputPath = CsvUtils.getHistortyFilePath(dir, params.taskId);
            params.historyWriter = new CsvHistoryWriter(outputPath);
            LOG.info("task id = {}, start = {}, end = {}, number of history = {}", params.taskId, new Timestamp(params.start), new Timestamp(params.end), params.numberOfHistory);
        }
        generateHistory(paramsList);
    }

    /**
     * 通話履歴生成タスクのパラメータを作成する
     *
     * @param recordsPerTask 1タスクが生成するレコード数
     * @return
     */
    List<Params> createParamsList(int recordsPerTask) {
        Date minDate = config.historyMinDate;
        Date maxDate = config.historyMaxDate;

        statistics = new Statistics(minDate, maxDate);

        List<Duration> durationList = ContractInfoReader.initDurationList(config);
        if (!isValidDurationList(durationList, minDate, maxDate)) {
            throw new RuntimeException("Invalid duration list.");
        }

        long numberOfTasks = (config.numberOfHistoryRecords + recordsPerTask - 1)
                / recordsPerTask;
        if (numberOfTasks > Integer.MAX_VALUE) {
            throw new RuntimeException("Too many numberOfTasks: " + numberOfTasks);
        }

        List<Params> paramsList = createTaskParams(minDate.getTime(), maxDate.getTime(),
                config.numberOfHistoryRecords, (int)numberOfTasks);
        return paramsList;
    }



    // 終了済みのタスク数
    private int numberOfEndTasks = 0;


    /**
     * 通話履歴を生成するタスクを実行し、生成された通話履歴を書き出す
     *
     * @throws IOException
     */
    public void generateHistory(List<Params> paramsList) {

        // 通話履歴を生成するタスクとスレッドの生成
        ExecutorService service = Executors.newFixedThreadPool(config.createTestDataThreadCount);

        int numberOfTasks = paramsList.size();
        Set<Future<Result>> futureSet = new HashSet<>(paramsList.size());
        numberOfEndTasks = 0;
        for (Params params : paramsList) {
            GenerateHistoryTask task = new GenerateHistoryTask(params);
            Future<Result> future = service.submit(task);
            futureSet.add(future);
            waitFor(numberOfTasks, futureSet);
        }
        LOG.info(String.format("%d tasks sumbitted.", numberOfTasks));

        service.shutdown();

        while(!futureSet.isEmpty()) {
            waitFor(numberOfTasks, futureSet);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // Noting to do
            }
        }
    }

    /**
     * 終了したタスクを調べる
     *
     * @param numberOfTasks
     * @param futureSet
     */
    @SuppressFBWarnings("DM_EXIT")
    private void waitFor(int numberOfTasks, Set<Future<Result>> futureSet) {
        Iterator<Future<Result>> it = futureSet.iterator();
        while (it.hasNext()) {
            Future<Result> future = it.next();
            if (future.isDone()) {
                Result result;
                try {
                    result = future.get();
                } catch (InterruptedException | ExecutionException e) {
                    result = new Result(-1);
                    result.success = false;
                    result.e = e;
                }
                if (!result.success) {
                    LOG.error("Task(id = {}) finished with error, aborting...", result.taskId, result.e);
                    System.exit(1);
                }
                it.remove();
                LOG.info(String.format("%d/%d tasks finished.", ++numberOfEndTasks, numberOfTasks));
            }
        }
    }

    /**
     * minDate～maxDateの間の全ての日付に対して、当該日付を含むdurationがlistに二つ以上あることを確認する
     *
     * @param list
     * @param minDate
     * @param maxDate
     */
    static boolean isValidDurationList(List<Duration> list, Date minDate, Date maxDate) {
        if (minDate.getTime() > maxDate.getTime()) {
            return false;
        }
        for (Date date = minDate; date.getTime() <= maxDate.getTime(); date = DateUtils.nextDate(date)) {
            int c = 0;
            for (Duration duration : list) {
                long start = duration.start;
                long end = duration.end == null ? Long.MAX_VALUE : duration.end;
                if (start <= date.getTime() && date.getTime() <= end) {
                    c++;
                    if (c >= 2) {
                        break;
                    }
                }
            }
            if (c < 2) {
                System.err.println("Duration List not contains date: " + date);
                return false;
            }
        }
        return true;
    }

    /**
     * @return statistics
     */
    protected Statistics getStatistics() {
        return statistics;
    }

    /**
     * @param statisticsOnly セットする statisticsOnly
     */
    protected void setStatisticsOnly(boolean statisticsOnly) {
        this.statisticsOnly = statisticsOnly;
    }


    /**
     * 通話履歴を書き出す抽象クラス.
     */
    public abstract class HistoryWriter {
        /**
         * クラスを初期化する
         * @throws IOException
         */
        abstract void init() throws IOException;


        /**
         * 1レコード分出力する
         * @throws IOException
         */
        abstract void write(History h) throws IOException;

        /**
         * クリーンナップ処理
         * @throws IOException
         */
        abstract void cleanup() throws IOException;
    }

    /**
     * CSV出力用クラス
     *
     */
    private class CsvHistoryWriter extends HistoryWriter {
        private BufferedWriter bw;
        private StringBuilder sb;
        private Path outputPath;

        public CsvHistoryWriter(Path outputPath) {
                this.outputPath = outputPath;
        }

        @Override
        void init() throws IOException {
            bw = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);
            sb = new StringBuilder();
        }

        @Override
        void write(History h) throws IOException {
            sb.setLength(0);
            sb.append(h.getCallerPhoneNumber());
            sb.append(',');
            sb.append(h.getRecipientPhoneNumber());
            sb.append(',');
            sb.append(h.getPaymentCategorty());
            sb.append(',');
            sb.append(h.getStartTime());
            sb.append(',');
            sb.append(h.getTimeSecs());
            sb.append(',');
            if (h.getCharge() != null) {
                sb.append(h.getCharge());
            }
            sb.append(',');
            sb.append(h.getDf());
            bw.write(sb.toString());
            bw.newLine();
        }

        @Override
        void cleanup() throws IOException {
            if (bw != null) {
                bw.close();
            }
        }
    }

    /**
     * DB出力用クラス
     *
     */
    private class DaoHistoryWriter extends HistoryWriter {
        PhoneBillDbManager manager;
        HistoryDao historyDao;
        List<History> histories = null;
        Config config;

        public DaoHistoryWriter(Config  config) {
            this.config = config;
        }

        @Override
        void init() throws IOException {
            histories = new ArrayList<History>(SQL_BATCH_EXEC_SIZE);
            manager = PhoneBillDbManager.createPhoneBillDbManager(config);
            historyDao = manager.getHistoryDao();
        }

        @Override
        void cleanup() throws IOException {
            if (histories.size() != 0) {
                insertHistories();
            }
            if (manager != null) {
                manager.close();
            }
        }

        @Override
        void write(History h) throws IOException {
            if (statisticsOnly) {
                statistics.addHistoy(h);
            } else {
                histories.add(h);
                if (histories.size() >= SQL_BATCH_EXEC_SIZE) {
                    insertHistories();
                }
            }
        }

        private void insertHistories() {
            manager.execute(TxOption.ofOCC(Integer.MAX_VALUE, TxLabel.TEST_DATA_GENERATOR), () -> {
                historyDao.batchInsert(histories);
            });
            histories.clear();
        }
    }


    /**
     * オンラインアプリケーション用のダミーのHistoryWriter
     *
     */
    private class DummyHistoryWriter  extends HistoryWriter {

        @Override
        void init() throws IOException {
        }

        @Override
        void write(History h) throws IOException {
        }

        @Override
        void cleanup() throws IOException {
        }

    }


    /**
     * 通話履歴のCSVファイルのパスを取得する
     *
     * @param dir CSVファイルのディレクトリ
     * @return
     */
    public static List<Path> getHistortyFilePaths(Path dir) {
        return null;
    }


    /**
     * 契約のCSVSファイルのパスを取得する
     *
     * @param dir CSVファイルのディレクトリ
     * @return
     */
    public static Path getContractsFilePath(Path dir) {
        return null;
    }


    /**
     * n番目の通話履歴のCSVファイルのパスを取得する
     *
     * @param n
     * @param dir CSVファイルのディレクトリ
     * @return
     */
    public static Path getHistortyFilePath(Path dir, int n) {
        return null;
    }
}
