package com.tsurugidb.benchmark.phonebill.app.billing;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.AbstractJdbcTestCase;
import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.Config.TransactionScope;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.entity.History.Key;
import com.tsurugidb.benchmark.phonebill.db.jdbc.Duration;
import com.tsurugidb.benchmark.phonebill.online.AbstractOnlineApp;
import com.tsurugidb.benchmark.phonebill.online.HistoryInsertApp;
import com.tsurugidb.benchmark.phonebill.online.HistoryUpdateApp;
import com.tsurugidb.benchmark.phonebill.online.MasterDeleteInsertApp;
import com.tsurugidb.benchmark.phonebill.online.MasterUpdateApp;
import com.tsurugidb.benchmark.phonebill.testdata.AbstractContractBlockInfoInitializer;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.CreateTestData;
import com.tsurugidb.benchmark.phonebill.testdata.DefaultContractBlockInfoInitializer;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

class PhoneBillTest extends AbstractJdbcTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneBillTest.class);
    private static String ORACLE_CONFIG_PATH = "src/test/config/oracle.properties";
    private static String ICEAXE_CONFIG = "src/test/config/iceaxe.properties";
    @Test
    void test() throws Exception {
        // 初期化
        Config config = Config.getConfig();
        new CreateTable().execute(config);
        PhoneBill phoneBill = new PhoneBill();
        phoneBill.config = config;

        // データが存在しない状態での料金計算
        phoneBill.doCalc(DateUtils.toDate("2020-11-01"), DateUtils.toDate("2020-11-30"));
        assertEquals(0, getBillings().size());

        // 契約マスタにテストデータをセット
        insertToContracts("Phone-0001", "2010-01-01", null, "Simple"); 			// 有効な契約
        insertToContracts("Phone-0002", "2010-01-01", "2020-10-31", "Simple"); 	// 終了した契約(境界値)
        insertToContracts("Phone-0003", "2010-01-01", "2020-11-01", "Simple");	// 有効な契約2(境界値)
        insertToContracts("Phone-0004", "2020-11-30", null, "Simple"); 			// 有効な契約3(境界値)
        insertToContracts("Phone-0005", "2020-11-30", "2021-01-10", "Simple"); 	// 有効な契約4(境界値)
        insertToContracts("Phone-0006", "2020-12-01", "2021-01-10", "Simple"); 	// 未来の契約(境界値)
        insertToContracts("Phone-0007", "2020-12-01", null, "Simple"); 			// 未来の契約(境界値)
        insertToContracts("Phone-0008", "2010-01-01", "2018-11-10", "Simple"); 	// 同一電話番号の複数の契約
        insertToContracts("Phone-0008", "2020-01-21", null, "Simple"); 			// 同一電話番号の複数の契約


        // 通話履歴がない状態での料金計算
        phoneBill.doCalc(DateUtils.toDate("2020-11-01"), DateUtils.toDate("2020-11-30"));
        List<Billing> billings = getBillings();
        assertEquals(5, billings.size());
        assertEquals(Billing.create("Phone-0001", "2020-11-01", 3000, 0, 3000, null), billings.get(0));
        assertEquals(Billing.create("Phone-0003", "2020-11-01", 3000, 0, 3000, null), billings.get(1));
        assertEquals(Billing.create("Phone-0004", "2020-11-01", 3000, 0, 3000, null), billings.get(2));
        assertEquals(Billing.create("Phone-0005", "2020-11-01", 3000, 0, 3000, null), billings.get(3));
        assertEquals(Billing.create("Phone-0008", "2020-11-01", 3000, 0, 3000, null), billings.get(4));
        List<History> histories = getHistories();
        assertEquals(0, histories.size());


        // 通話履歴ありの場合
        insertToHistory("Phone-0001", "Phone-0008", "C", "2020-10-31 23:59:59.999", 30, 0);		// 計算対象年月外
        insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:00:00.000", 30, 0);  	// 計算対象
        insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-15 12:12:12.000", 90, 1); 	 	// 削除フラグ
        insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-30 23:59:59.999", 90, 0);  	// 計算対象
        insertToHistory("Phone-0001", "Phone-0008", "C", "2020-12-01 00:00:00.000", 30, 0);  	// 計算対象年月外
        insertToHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 00:00:00.000", 250, 0);  	// 計算対象
        insertToHistory("Phone-0005", "Phone-0008", "R", "2020-11-30 00:00:00.000", 30, 0);  	// 計算対象(受信者負担)

        phoneBill.doCalc(DateUtils.toDate("2020-11-01"), DateUtils.toDate("2020-11-30"));
        billings = getBillings();
        assertEquals(5, billings.size());
        assertEquals(Billing.create("Phone-0001", "2020-11-01", 3000, 30, 3000, null), billings.get(0));
        assertEquals(Billing.create("Phone-0003", "2020-11-01", 3000, 0, 3000, null), billings.get(1));
        assertEquals(Billing.create("Phone-0004", "2020-11-01", 3000, 0, 3000, null), billings.get(2));
        assertEquals(Billing.create("Phone-0005", "2020-11-01", 3000, 50, 3000, null), billings.get(3));
        assertEquals(Billing.create("Phone-0008", "2020-11-01", 3000, 10, 3000, null), billings.get(4));
        histories = getHistories();
        assertEquals(7, histories.size());
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-10-31 23:59:59.999", 30, null, 0), histories.get(0));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:00:00.000", 30, 10, 0), histories.get(1));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-15 12:12:12.000", 90, null, 1), histories.get(2));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-30 23:59:59.999", 90, 20, 0), histories.get(3));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-12-01 00:00:00.000", 30, null, 0), histories.get(4));
        assertEquals(toHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 00:00:00.000", 250, 50, 0), histories.get(5));
        assertEquals(toHistory("Phone-0005", "Phone-0008", "R", "2020-11-30 00:00:00.000", 30, 10, 0), histories.get(6));


        billings = getBillings();
        assertEquals(5, billings.size());


        // Exception 発生時にrollbackされることの確認
        // Phone-0001が先に処理されテーブルが更新されるが、Phone-005の処理でExceptionが発生し、処理全体がロールバックされる
        insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:30:00.000", 30, 0);  	// 計算対象
        insertToHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 01:00:00.000", -1, 0);  	// 通話時間が負数なのでExceptionがスローされる
        RuntimeException re =  assertThrows(RuntimeException.class, () ->
        phoneBill.doCalc(DateUtils.toDate("2020-11-01"), DateUtils.toDate("2020-11-30")));
        assertEquals("Negative time: -1", re.getMessage());
        billings = getBillings();
        // 既存の請求データの削除処理は他の処理と別トランザクションのためロールバックされず削除された状態になる
        assertEquals(0, billings.size());

        // 通話履歴も更新されていないことを確認
        histories = getHistories();
        assertEquals(9, histories.size());
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-10-31 23:59:59.999", 30, null, 0), histories.get(0));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:00:00.000", 30, 10, 0), histories.get(1));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:30:00.000", 30, null, 0), histories.get(2));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-15 12:12:12.000", 90, null, 1), histories.get(3));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-30 23:59:59.999", 90, 20, 0), histories.get(4));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-12-01 00:00:00.000", 30, null, 0), histories.get(5));
        assertEquals(toHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 00:00:00.000", 250, 50, 0), histories.get(6));
        assertEquals(toHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 01:00:00.000", -1, null, 0), histories.get(7));
        assertEquals(toHistory("Phone-0005", "Phone-0008", "R", "2020-11-30 00:00:00.000", 30, 10, 0), histories.get(8));


        // Exceptionの原因となるレコードを削除して再実行
        String sql = "delete from history where caller_phone_number = 'Phone-0005' and start_time = '2020-11-10 01:00:00'";
        getStmt().execute(sql);
        phoneBill.doCalc( DateUtils.toDate("2020-11-01"), DateUtils.toDate("2020-11-30"));
        billings = getBillings();
        assertEquals(5, billings.size());
        assertEquals(Billing.create("Phone-0001", "2020-11-01", 3000, 40, 3000, null), billings.get(0));
        assertEquals(Billing.create("Phone-0003", "2020-11-01", 3000, 0, 3000, null), billings.get(1));
        assertEquals(Billing.create("Phone-0004", "2020-11-01", 3000, 0, 3000, null), billings.get(2));
        assertEquals(Billing.create("Phone-0005", "2020-11-01", 3000, 50, 3000, null), billings.get(3));
        assertEquals(Billing.create("Phone-0008", "2020-11-01", 3000, 10, 3000, null), billings.get(4));
        histories = getHistories();
        assertEquals(8, histories.size());
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-10-31 23:59:59.999", 30, null, 0), histories.get(0));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:00:00.000", 30, 10, 0), histories.get(1));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:30:00.000", 30, 10, 0), histories.get(2));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-15 12:12:12.000", 90, null, 1), histories.get(3));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-30 23:59:59.999", 90, 20, 0), histories.get(4));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-12-01 00:00:00.000", 30, null, 0), histories.get(5));
        assertEquals(toHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 00:00:00.000", 250, 50, 0), histories.get(6));
        assertEquals(toHistory("Phone-0005", "Phone-0008", "R", "2020-11-30 00:00:00.000", 30, 10, 0), histories.get(7));



        // 論理削除フラグを立てたレコードが計算対象外になることの確認
        sql = "update history set df = 1 where caller_phone_number = 'Phone-0001' and start_time = '2020-11-30 23:59:59.999'";
        getStmt().execute(sql);
        phoneBill.doCalc( DateUtils.toDate("2020-11-01"), DateUtils.toDate("2020-11-30"));
        billings = getBillings();
        assertEquals(5, billings.size());
        assertEquals(Billing.create("Phone-0001", "2020-11-01", 3000, 20, 3000, null), billings.get(0));
        assertEquals(Billing.create("Phone-0003", "2020-11-01", 3000, 0, 3000, null), billings.get(1));
        assertEquals(Billing.create("Phone-0004", "2020-11-01", 3000, 0, 3000, null), billings.get(2));
        assertEquals(Billing.create("Phone-0005", "2020-11-01", 3000, 50, 3000, null), billings.get(3));
        assertEquals(Billing.create("Phone-0008", "2020-11-01", 3000, 10, 3000, null), billings.get(4));
        histories = getHistories();
        assertEquals(8, histories.size());
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-10-31 23:59:59.999", 30, null, 0), histories.get(0));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:00:00.000", 30, 10, 0), histories.get(1));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:30:00.000", 30, 10, 0), histories.get(2));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-15 12:12:12.000", 90, null, 1), histories.get(3));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-30 23:59:59.999", 90, 20, 1), histories.get(4));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-12-01 00:00:00.000", 30, null, 0), histories.get(5));
        assertEquals(toHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 00:00:00.000", 250, 50, 0), histories.get(6));
        assertEquals(toHistory("Phone-0005", "Phone-0008", "R", "2020-11-30 00:00:00.000", 30, 10, 0), histories.get(7));
    }



    /**
     * toDuration()のテスト
     */
    @Test
    void testToDuration() {
        Duration d;

        d = PhoneBill.toDuration(DateUtils.toDate("2020-12-01"));
        assertEquals(DateUtils.toDate("2020-12-01"), d.getStatDate());
        assertEquals(DateUtils.toDate("2020-12-31"), d.getEndDate());

        d = PhoneBill.toDuration(DateUtils.toDate("2020-12-31"));
        assertEquals(DateUtils.toDate("2020-12-01"), d.getStatDate());
        assertEquals(DateUtils.toDate("2020-12-31"), d.getEndDate());

        d = PhoneBill.toDuration(DateUtils.toDate("2021-02-05"));
        assertEquals(DateUtils.toDate("2021-02-01"), d.getStatDate());
        assertEquals(DateUtils.toDate("2021-02-28"), d.getEndDate());

    }

    /*
     * Configに違いがあっても処理結果が変わらないことを確認
     */
    @Test
    void testConfigVariation() throws Exception {
        // まず実行し、その結果を期待値とする
        Config config = createConfigForTestConfigVariation();

        new CreateTable().execute(config);
        new CreateTestData().execute(config);
        PhoneBill phoneBill = new PhoneBill();
        phoneBill.execute(config);
        List<Billing> expected = getBillings();

        // スレッド数 、コネクション共有の有無で結果が変わらないことを確認
        boolean[] sharedConnections = { false }; 	// PostgreSQLのJDBCドライバはスレッドセーフでなく、コネクション
                                                         // を共有したときに正しい結果を得られないことがあるため、
                                                        // コネクション共有のセストは実施しない
        int[] threadCounts = { 1, 2, 4, 8, 16 };
        for (boolean sharedConnection : sharedConnections) {
            for (int threadCount : threadCounts) {
                Config newConfig = config.clone();
                newConfig.threadCount = threadCount;
                newConfig.sharedConnection = sharedConnection;
                LOG.info("Executing phoneBill.exec() with threadCount =" + threadCount + ", sharedConnection = "
                        + sharedConnection);
                testNewConfig(phoneBill, expected, newConfig);
            }
        }

        // トランザクションスコープの違いで結果が変わらないことの確認
        for(TransactionScope ts: TransactionScope.values()) {
            Config newConfig = config.clone();
            newConfig.transactionScope= ts;
            LOG.info("Executing phoneBill.exec() with transactionScope = " + ts);
            testNewConfig(phoneBill, expected, newConfig);
        }
    }


    private Config createConfigForTestConfigVariation() throws IOException {
        Config config = Config.getConfig();
        config.duplicatePhoneNumberRate = 10;
        config.expirationDateRate = 10;
        config.noExpirationDateRate = 70;
        config.numberOfContractsRecords = (int) 100;
        config.numberOfHistoryRecords = (int) 1000;
        config.threadCount = 1;
        config.sharedConnection = false;
        config.historyInsertThreadCount = 0;
        config.historyUpdateThreadCount = 0;
        config.masterUpdateThreadCount = 0;
        config.masterDeleteInsertThreadCount = 0;
        return config;
    }




    /*
     * Configに違いがあっても処理結果が変わらないことを確認(Oracle版)
     */
    @Test
    @Tag("oracle")
    void testConfigVariationForOracle() throws Exception {
        // PostgreSQLで実行し、その結果を期待値とする
        Config config = createConfigForTestConfigVariation();
        new CreateTable().execute(config);
        new CreateTestData().execute(config);
        PhoneBill phoneBill = new PhoneBill();
        phoneBill.execute(config);
        List<Billing> expected = getBillings();


        // Oracle用のconfigを作成

        Config oracleConfig = Config.getConfig(ORACLE_CONFIG_PATH);
        config = config.clone();
        config.url = oracleConfig.url;
        config.user = oracleConfig.user;
        config.password = oracleConfig.password;;
        config.dbmsType = oracleConfig.dbmsType;

        // Iceaxe用のテストデータを生成
        new CreateTable().execute(config);
        new CreateTestData().execute(config);

        // スレッド数 、コネクション共有の有無で結果が変わらないことを確認
        boolean[] sharedConnections = { false, true };
        int[] threadCounts = { 1, 2, 4, 8, 16 };
        for (boolean sharedConnection : sharedConnections) {
            for (int threadCount : threadCounts) {
                Config newConfig = config.clone();
                newConfig.threadCount = threadCount;
                newConfig.sharedConnection = sharedConnection;
                LOG.info("Executing phoneBill.exec() with threadCount =" + threadCount + ", sharedConnection = "
                        + sharedConnection);
                testNewConfig(phoneBill, expected, newConfig);
            }
        }

        // トランザクションスコープの違いで結果が変わらないことの確認
        for(TransactionScope ts: TransactionScope.values()) {
            Config newConfig = config.clone();
            newConfig.transactionScope= ts;
            LOG.info("Executing phoneBill.exec() with transactionScope = " + ts);
            testNewConfig(phoneBill, expected, newConfig);
        }
    }


    /*
     * Configに違いがあっても処理結果が変わらないことを確認(Iceaxe版)
     */
    @Test
    @Disabled("This test is disabled temporarily due to long processing time.")
    void testConfigVariationForIceaxe() throws Exception {
        // PostgreSQLで実行し、その結果を期待値とする
        Config config = createConfigForTestConfigVariation();
        new CreateTable().execute(config);
        new CreateTestData().execute(config);
        PhoneBill phoneBill = new PhoneBill();
        phoneBill.execute(config);
        List<Billing> expected = getBillings();


        // Iceaxe用のconfigを作成
        Config iceaxeConfig = Config.getConfig(ICEAXE_CONFIG);
        config = config.clone();
        config.url = iceaxeConfig.url;
        config.user = iceaxeConfig.user;
        config.password = iceaxeConfig.password;;
        config.dbmsType = iceaxeConfig.dbmsType;

        // Iceaxe用のテストデータを生成
        new CreateTable().execute(config);
        new CreateTestData().execute(config);

        // スレッド数 、コネクション共有の有無で結果が変わらないことを確認
        boolean[] sharedConnections = { false };
        int[] threadCounts = { 1 };
        for (boolean sharedConnection : sharedConnections) {
            for (int threadCount : threadCounts) {
                Config newConfig = config.clone();
                newConfig.threadCount = threadCount;
                newConfig.sharedConnection = sharedConnection;
                LOG.info("Executing phoneBill.exec() with threadCount =" + threadCount + ", sharedConnection = "
                        + sharedConnection);
                testNewConfig(phoneBill, expected, newConfig);
            }
        }

        // トランザクションスコープの違いで結果が変わらないことの確認
        for(TransactionScope ts: TransactionScope.values()) {
            Config newConfig = config.clone();
            newConfig.transactionScope= ts;
            LOG.info("Executing phoneBill.exec() with transactionScope = " + ts);
            testNewConfig(phoneBill, expected, newConfig);
        }
    }


    /**
     * @param phoneBill
     * @param expected
     * @param newConfig
     * @throws Exception
     * @throws SQLException
     */
    private void testNewConfig(PhoneBill phoneBill, List<Billing> expected, Config newConfig)
            throws Exception, SQLException {
        phoneBill.execute(newConfig);
        List<Billing> actual = getBillings();
        if (expected.size() != 76) {
            System.out.println(actual);
        }
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals( expected.get(i), actual.get(i), Integer.toString(i));
        }
    }


    @Test
    public void testCreateOnlineApps() throws Exception {
        Config config = Config.getConfig();
        new CreateTestData().execute(config);
        AbstractContractBlockInfoInitializer infoInitializer = new DefaultContractBlockInfoInitializer(config);
        ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager(infoInitializer);

        // オンラインアプリを動かさないケース(1分間に実行する回数が0)
        config.historyInsertTransactionPerMin = 0;
        config.historyUpdateRecordsPerMin = 0;
        config.masterDeleteInsertRecordsPerMin = 0;
        config.masterUpdateRecordsPerMin = 0;
        config.historyInsertThreadCount = 1;
        config.historyUpdateThreadCount = 1;
        config.masterDeleteInsertThreadCount = 1;
        config.masterUpdateThreadCount = 1;
        assertEquals(Collections.emptyList(), PhoneBill.createOnlineApps(config, accessor));

        // オンラインアプリを動かさないケース(スレッド数の指定が0)
        config.historyInsertTransactionPerMin = 1;
        config.historyUpdateRecordsPerMin = 1;
        config.masterDeleteInsertRecordsPerMin = 1;
        config.masterUpdateRecordsPerMin = 1;
        config.historyInsertThreadCount = 0;
        config.historyUpdateThreadCount = 0;
        config.masterDeleteInsertThreadCount = 0;
        config.masterUpdateThreadCount = 0;
        assertEquals(Collections.emptyList(), PhoneBill.createOnlineApps(config, accessor));

        // スレッド数で指定された数だけ、オンラインアプリが作成されていることを確認する
        config.historyInsertTransactionPerMin = 1;
        config.historyUpdateRecordsPerMin = 1;
        config.masterDeleteInsertRecordsPerMin = 1;
        config.masterUpdateRecordsPerMin = 1;
        config.historyInsertThreadCount = 1;
        config.historyUpdateThreadCount = 2;
        config.masterDeleteInsertThreadCount = 3;
        config.masterUpdateThreadCount = 4;

        // オンラインアプリがconfigで指定された数だけ作成されることの確認
        Map<Class<?>, List<AbstractOnlineApp>> map = new HashMap<>();
        map.put(HistoryInsertApp.class, new ArrayList<AbstractOnlineApp>());
        map.put(HistoryUpdateApp.class, new ArrayList<AbstractOnlineApp>());
        map.put(MasterDeleteInsertApp.class, new ArrayList<AbstractOnlineApp>());
        map.put(MasterUpdateApp.class, new ArrayList<AbstractOnlineApp>());
        for(AbstractOnlineApp app: PhoneBill.createOnlineApps(config, accessor)) {
            List<AbstractOnlineApp>	list = map.get(app.getClass());
            list.add(app);
        }

        // 生成されたオンラインアプリの数とnameの確認
        checkAppList(1, "HistoryInsertApp", map.get(HistoryInsertApp.class));
        checkAppList(2, "HistoryUpdateApp", map.get(HistoryUpdateApp.class));
        checkAppList(3, "MasterDeleteInsertApp", map.get(MasterDeleteInsertApp.class));
        checkAppList(4, "MasterUpdateApp", map.get(MasterUpdateApp.class));

        // 契約のブロックが不足してExceptionが発生するケース
        SingleProcessContractBlockManager accessor2 = new SingleProcessContractBlockManager(); // Initializerを指定しないと契約マスタが空の状態に
                                                            // 合致するインスタンスを生成される
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> PhoneBill.createOnlineApps(config, accessor2));
        assertEquals("Insufficient test data, create test data first.", e.getMessage());
    }

    private void checkAppList(int expectedSize, String prefix, List<AbstractOnlineApp> list) {
        assertEquals(expectedSize, list.size());
        for(int i = 0; i < list.size(); i++) {
            String expected = prefix + "-00" + i;
            String actual = list.get(i).getName();
            assertEquals(expected, actual);
        }
    }

    /**
     * オンラインアプリとバッチを同時に動かすケース
     * @throws Exception
     */
    @Test
    public void runWithOnlineApp() throws Exception {
        Config config = Config.getConfig();
        new CreateTable().execute(config);
        new CreateTestData().execute(config);

        config.historyInsertTransactionPerMin = -1;

        List<History> historiesBefore = getHistories();
        List<Contract> contractsBefore = getContracts();

        Config phoneBillConfig = Config.getConfig("src/test/config/phone_bill_test.properties");
        PhoneBill phoneBill = new PhoneBill();
        phoneBill.execute(phoneBillConfig);

        List<History> historiesAfter = getHistories();
        List<Contract> contractsAfter = getContracts();

        // オンラインアプリによりHistoryテーブルのレコード数が増えていることを確認
        assertTrue(historiesAfter.size() > historiesBefore.size());

        // オンラインアプリが動いてもContractsテーブルのレコード数が変わらないか1レコード減っていることを確認する。
        assertTrue(contractsAfter.size() ==  historiesBefore.size() ||
                contractsAfter.size() ==  historiesBefore.size() -1
                );
        System.out.println("historiesBeforeSize = " + historiesBefore.size());
        System.out.println("historiesAfterSize = " + historiesAfter.size());
        System.out.println("contractsBeforeSize = " + contractsBefore.size());
        System.out.println("contractsAfterSize = " + contractsAfter.size());


        // オンラインアプリにより契約終了日が削除されているレコードが存在することを確認
        boolean exist = false;
        for (int i = 0; i < contractsBefore.size(); i++) {
            Contract before = contractsBefore.get(i);
            Contract after = contractsAfter.get(i);
            if (before.getEndDate() == null && after.getEndDate() != null) {
                exist = true;
                System.out.println("before = " + before);
                System.out.println("after  = " + after);
                break;
            }
        }
        assertTrue(exist);

        // オンラインアプリにより契約終了日が更新されているレコードが存在することを確認
        exist = false;
        for (int i = 0; i < contractsBefore.size(); i++) {
            Contract before = contractsBefore.get(i);
            Contract after = contractsAfter.get(i);
            if (before.getEndDate() != null && after.getEndDate() != null && !before.getEndDate().equals(after.getEndDate())) {
                exist = true;
                System.out.println("before = " + before);
                System.out.println("after  = " + after);
                break;
            }
        }

        // オンラインアプリにより削除フラグが立っているレコードと、通話時間が変更されているレコードが存在することを確認する
        Map<Key, History> map = 	historiesAfter.stream().collect(Collectors.toMap(History::getKey, h -> h));


        exist = false;
        for (int i = 0; i < historiesBefore.size(); i++) {
            History before = historiesBefore.get(i);
            History after = map.get(before.getKey());
            if (before.getDf() == 0 && after.getDf() == 1) {
                exist = true;
                System.out.println("before = " + before);
                System.out.println("after  = " + after);
                break;
            }
        }
        assertTrue(exist);

        exist = false;
        for (int i = 0; i < historiesBefore.size(); i++) {
            History before = historiesBefore.get(i);
            History after = map.get(before.getKey());
            if (before.getTimeSecs() != after.getTimeSecs()) {
                exist = true;
                System.out.println("before = " + before);
                System.out.println("after  = " + after);
                break;
            }
        }
        assertTrue(exist);
    }

    /**
     * {@link Config#onlineOnly}が期待通り動作することのテスト
     * @throws Exception
     */
    @Test
    public void testConfigOnlineOnly() throws Exception {
        Config config = Config.getConfig();
        new CreateTestData().execute(config);
        TestPhoneBill phoneBill;

        // onlineOnly = false
        config.onlineOnly = false;
        phoneBill = new TestPhoneBill();
        config.execTimeLimitSecs = 1;
        phoneBill.execute(config);
        assertTrue(phoneBill.doCalcCalled);

        // onlineOnly = true
        config.onlineOnly = true;
        config.execTimeLimitSecs = 1;
        config.historyInsertTransactionPerMin = 1;
        config.historyUpdateRecordsPerMin = 1;
        config.masterDeleteInsertRecordsPerMin = 1;
        config.masterUpdateRecordsPerMin = 1;
        phoneBill = new TestPhoneBill();
        phoneBill.execute(config);
        assertFalse(phoneBill.doCalcCalled);

        // オンラインアプリなしでonlineOnlyで動かした場合
        config.historyInsertTransactionPerMin = 0;
        config.historyUpdateRecordsPerMin = 0;
        config.masterDeleteInsertRecordsPerMin = 0;
        config.masterUpdateRecordsPerMin = 0;
        config.onlineOnly = true;
        phoneBill = new TestPhoneBill();
        phoneBill.execute(config);
        assertFalse(phoneBill.doCalcCalled);
    }

    /**
     * {@link Config#execTimeLimitSecs}が期待通り動作することのテスト
     * @throws Exception
     */
    @Test
    public void testExecTimeLimitSecs() throws Exception {
        Config config = Config.getConfig();
        new CreateTestData().execute(config);
        TestPhoneBill phoneBill;

        // onlineOnly = false
        config.onlineOnly = false;
        phoneBill = new TestPhoneBill();
        config.execTimeLimitSecs = 1;
        long start = System.currentTimeMillis();
        phoneBill.execute(config);
        long duration = System.currentTimeMillis() - start;
        assertTrue(phoneBill.doCalcCalled);
        assertEquals(1100d, (long)duration, 100d);

        // onlineOnly = true
        config.onlineOnly = true;
        config.execTimeLimitSecs = 2;
        config.historyInsertTransactionPerMin = 1;
        config.historyUpdateRecordsPerMin = 1;
        config.masterDeleteInsertRecordsPerMin = 1;
        config.masterUpdateRecordsPerMin = 1;
        start = System.currentTimeMillis();
        phoneBill = new TestPhoneBill();
        phoneBill.execute(config);
        duration = System.currentTimeMillis() - start;
        assertFalse(phoneBill.doCalcCalled);
        assertEquals(2100d, (long)duration, 100d);

        // オンラインアプリなしでonlineOnlyで動かした場合
        config.onlineOnly = true;
        config.execTimeLimitSecs = 1;
        config.historyInsertTransactionPerMin = 0;
        config.historyUpdateRecordsPerMin = 0;
        config.masterDeleteInsertRecordsPerMin = 0;
        config.masterUpdateRecordsPerMin = 0;
        start = System.currentTimeMillis();
        phoneBill = new TestPhoneBill();
        phoneBill.execute(config);
        duration = System.currentTimeMillis() - start;
        assertFalse(phoneBill.doCalcCalled);
        assertEquals(200d, (long)duration, 200d);
    }


    /**
     * doCalcの呼び出し状況を確認するためのクラス
     */
    private static class TestPhoneBill extends PhoneBill {
        boolean doCalcCalled = false;

        @Override
        void doCalc(Date start, Date end) throws Exception {
            doCalcCalled = true;
            while (!getAbortRequested()) {
                Thread.sleep(10);
            }
        }
    }

}

