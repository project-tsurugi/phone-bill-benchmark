/**
 *
 */
package com.tsurugidb.benchmark.phonebill.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.AbstractJdbcTestCase;
import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.jdbc.Duration;
import com.tsurugidb.benchmark.phonebill.testdata.GenerateHistoryTask.Params;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;
import com.tsurugidb.benchmark.phonebill.util.PathUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

class TestDataGeneratorTest extends AbstractJdbcTestCase {
    private Path tempDir = null;


    @BeforeEach
    void createTempDir() throws IOException {
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrwx");
        tempDir = Files.createTempDirectory("csv");
        FileStore fileStore = Files.getFileStore(tempDir);
        if (fileStore.supportsFileAttributeView(PosixFileAttributeView.class)) {
            Files.setPosixFilePermissions(tempDir, perms);
        }
    }

    @AfterEach
    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    void cleanupTempDir() throws IOException {
        if (Files.isDirectory(tempDir)) {
            for(File file: tempDir.toFile().listFiles()) {
                if (!file.delete()) {
                    throw new IOException("Fail to delete a file: " + file.toString());
                }
            }
        }
        Files.deleteIfExists(tempDir);
    }

    /**
     * isValidDurationList()のテスト
     */
    @Test
    void isValidDurationList() {
        List<Duration> list = new ArrayList<Duration>();
        // listの要素が0と1のときは常にfalseが返る
        assertFalse(isValidDurationListSub(list, "2010-11-11", "2010-11-11"));
        list.add(toDuration("2010-10-11", "2010-11-25"));
        assertFalse(isValidDurationListSub(list, "2010-11-11", "2010-11-11"));

        // listの要素が2、開始日、終了日の境界値のテスト
        list.add(toDuration("2010-11-18", "2010-11-30"));
        assertTrue(isValidDurationListSub(list, "2010-11-18", "2010-11-25"));
        assertFalse(isValidDurationListSub(list, "2010-11-18", "2010-11-26"));
        assertFalse(isValidDurationListSub(list, "2010-11-17", "2010-11-25"));

        // 開始日==終了日のとき、開始日と終了日が逆転しているとき
        assertTrue(isValidDurationListSub(list, "2010-11-19", "2010-11-19"));
        assertFalse(isValidDurationListSub(list, "2010-11-28", "2010-11-18"));

        // 終了日がnullのデータがあるケース
        list.get(1).end = null;
        assertTrue(isValidDurationListSub(list, "2010-11-18", "2010-11-25"));
        assertFalse(isValidDurationListSub(list, "2010-11-18", "2010-11-26"));
        assertFalse(isValidDurationListSub(list, "2010-11-17", "2010-11-25"));

    }

    boolean isValidDurationListSub(List<Duration> list, String start, String end) {
        return TestDataGenerator.isValidDurationList(list, DateUtils.toDate(start), DateUtils.toDate(end));
    }


    /**
     * generateContractToDb()のテスト
     * @throws Exception
     */
    @Test
    void testGenerateContractToDb() throws Exception {
        new CreateTable().execute(Config.getConfig());

        Config config = Config.getConfig();
        config.minDate = DateUtils.toDate("2010-01-11");
        config.maxDate = DateUtils.toDate("2020-12-21");
        config.numberOfContractsRecords = 10000;
        config.expirationDateRate =5;
        config.noExpirationDateRate = 11;
        config.duplicatePhoneNumberRate = 2;

        int seed = config.randomSeed;
        ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
        TestDataGenerator generator = new TestDataGenerator(config, new Random(seed), accessor);
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            generator.generateContractsToDb(manager);
        }

        String sql;

        // 100レコード生成されていること
        sql = "select count(*) from contracts";
        assertEquals("10000", execSqlAndGetString(sql));

        // 複数のレコードを持つ電話番号が1000種類存在すること
        sql = "select count(*) from  "
                + "(select phone_number, count(*) from contracts group by phone_number "
                + " having count(*) > 1) as dummy ";
        assertEquals("1000", execSqlAndGetString(sql));

        // end_dateを持たないレコードが6500であること
        sql = "select count(*) from contracts where end_date is null";
        assertEquals("6500", execSqlAndGetString(sql));


    }

    private String execSqlAndGetString(String sql) throws SQLException {
        try (ResultSet rs = getStmt().executeQuery(sql)) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        throw new SQLException("Fail to exece sql:" + sql);
    }




    /**
     * getCommonDuration()のテスト
     */
    @Test
    void testGetCommonDuration() {
        // 共通の期間がないケース(期間が連続していない)
        assertNull(testGetCommonDurationSub("2020-01-01", "2020-01-03", "2020-01-05", "2020-01-07"));
        assertNull(testGetCommonDurationSub("2020-02-01", "2020-02-03", "2020-01-05", "2020-01-07"));
        // 共通の期間がないケース(期間が連続している)
        assertNull(testGetCommonDurationSub("2020-01-01", "2020-01-03", "2020-01-04", "2020-01-07"));
        assertNull(testGetCommonDurationSub("2020-01-08", "2020-02-03", "2020-01-05", "2020-01-07"));
        // 1日だけ共通の期間があるケース
        assertEquals(toDuration("2020-01-03", "2020-01-03"),
                testGetCommonDurationSub("2020-01-01", "2020-01-03", "2020-01-03", "2020-01-07"));
        assertEquals(toDuration("2020-01-07", "2020-01-07"),
                testGetCommonDurationSub("2020-01-07", "2020-02-03", "2020-01-05", "2020-01-07"));
        // 複数の共通の期間があるケース
        // 1日だけ共通の期間があるケース
        assertEquals(toDuration("2020-01-03", "2020-01-05"),
                testGetCommonDurationSub("2020-01-01", "2020-01-05", "2020-01-03", "2020-01-07"));
        assertEquals(toDuration("2020-01-07", "2020-01-09"),
                testGetCommonDurationSub("2020-01-07", "2020-02-03", "2020-01-05", "2020-01-09"));

        // 一方が他方の期間を完全に含むケース(開始、終了のどちらも一致しない)
        assertEquals(toDuration("2020-01-03", "2020-01-05"),
                testGetCommonDurationSub("2020-01-01", "2020-02-05", "2020-01-03", "2020-01-05"));
        assertEquals(toDuration("2020-01-07", "2020-02-03"),
                testGetCommonDurationSub("2020-01-07", "2020-02-03", "2020-01-05", "2020-02-09"));
        // 一方が他方の期間を完全に含むケース(開始、終了のどちかが一致)
        assertEquals(toDuration("2020-01-03", "2020-01-05"),
                testGetCommonDurationSub("2020-01-01", "2020-01-05", "2020-01-03", "2020-01-05"));
        assertEquals(toDuration("2020-01-07", "2020-02-03"),
                testGetCommonDurationSub("2020-01-07", "2020-02-03", "2020-01-05", "2020-02-03"));
        assertEquals(toDuration("2020-01-03", "2020-01-05"),
                testGetCommonDurationSub("2020-01-03", "2020-02-05", "2020-01-03", "2020-01-05"));
        assertEquals(toDuration("2020-01-07", "2020-02-03"),
                testGetCommonDurationSub("2020-01-07", "2020-02-03", "2020-01-07", "2020-02-09"));
        // 期間が完全に一致するケース
        assertEquals(toDuration("2020-01-07", "2020-02-03"),
                testGetCommonDurationSub("2020-01-07", "2020-02-03", "2020-01-07", "2020-02-03"));


    }

    private Duration testGetCommonDurationSub(String d1s, String d1e, String d2s, String d2e) {
        return TestDataGenerator.getCommonDuration(toDuration(d1s, d1e), toDuration(d2s, d2e));
    }

    private Duration toDuration(String start, String end) {
        return new Duration(DateUtils.toDate(start), DateUtils.toDate(end));
    }





    /**
     * generateContractsToCsv()のテスト
     * @throws Exception
     */
    @Test
    @Tag("copy-command")
    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    void generateContractsToCsv() throws Exception {

        // DBに生成したテストデータとCSVファイルに生成したテストデータが一致することを確認する
        Config config = Config.getConfig();
        config.csvDir = tempDir.toString();
        config.numberOfContractsRecords = 5154; // 間違ったデータを参照したときに件数で判別できるように、
                                                // このテストケース固有の値を指定する

        // テーブルを空にする
        new CreateTable().execute(config);
        getStmt().execute("truncate table contracts");


        // DBにデータを作成
        ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
        int seed = config.randomSeed;
        TestDataGenerator generator = new TestDataGenerator(config, new Random(seed), accessor);
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            generator.generateContractsToDb(manager);
        }
        Path expectedFilePath = tempDir.resolve("contracts.db").toAbsolutePath();
        String expectedFilePathString = PathUtils.toWls(expectedFilePath, File.separatorChar == '\\');

        getStmt().execute("copy contracts to '"+expectedFilePathString+"' with csv");
        List<String> expected = Files.readAllLines(expectedFilePath);
        Collections.sort(expected);

        // CSVを作成
        Path contracts = CsvUtils.getContractsFilePath(tempDir);
        accessor = new SingleProcessContractBlockManager();
        generator = new TestDataGenerator(config, new Random(seed), accessor);
        generator.generateContractsToCsv(contracts);
        List<String> actual = Files.readAllLines(contracts);
        Collections.sort(actual);

        // 比較
        assertIterableEquals(expected, actual);
    }

    /**
     * generateHistoryToCSV() のテスト
     * @throws IOException
     * @throws SQLException
     */
    @Test
    @Tag("copy-command")
    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    void gestGenerateHistoryToCSV() throws IOException, SQLException {
        // DBに生成したテストデータとCSVファイルに生成したテストデータが一致することを確認する
        Config config = Config.getConfig();
        config.csvDir = tempDir.toString();
        config.numberOfHistoryRecords = 3964; // 間違ったデータを参照したときに件数で判別できるように、
                                                // このテストケース固有の値を指定する
        getStmt().execute("truncate table history");

        ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
        TestDataGenerator g1 = new TestDataGenerator(config, new Random(config.randomSeed), accessor);
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            g1.generateContractsToDb(manager); // accessorの初期化のため契約データを作成する
            g1.generateHistoryToDb(config);
        }

        Path expectedFilePath = tempDir.resolve("history.db").toAbsolutePath();
        String expectedFilePathString = PathUtils.toWls(expectedFilePath, File.separatorChar == '\\');

        getStmt().execute("copy history to '"+expectedFilePathString+"' with csv");
        List<String> expected = Files.readAllLines(expectedFilePath);
        Collections.sort(expected);

        TestDataGenerator g2 = new TestDataGenerator(config, new Random(config.randomSeed), accessor);
        g2.generateHistoryToCsv(tempDir);
        List<String> actual = Files.readAllLines(CsvUtils.getHistortyFilePaths(tempDir).get(0));
        Collections.sort(actual);

        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            String e = expected.get(i);
            String a = actual.get(i).replaceAll("\\.0,", ","); // ミリ秒が0のときの表現の違いをreplaceAllで吸収する
            assertEquals(e, a);
        }
    }

    /*
     * createParamsList()のテスト.
     * <br>
     * paramsのstart, end, numberOfHistoryが期待した値になっていることを確認する
     */
    @Test
    void testCreateParamsList() throws IOException {
        Config config = Config.getConfig();

        // デフォルトコンフィグでテスト(numberOfHistoryRecordsがmaxNumberOfLinesHistoryCsvより小さいケース)
        ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
        TestDataGenerator generator = new TestDataGenerator(config, new Random(config.randomSeed), accessor);
        int numberOfParams = 0;
        for(Params params: generator.createParamsList(config.maxNumberOfLinesHistoryCsv)) {
            numberOfParams++;
            assertEquals(config.historyMinDate.getTime(), params.start);
            assertEquals(config.historyMaxDate.getTime(), params.end);
            assertEquals(config.numberOfHistoryRecords, params.numberOfHistory);
        }
        assertEquals(1, numberOfParams);

        // === numberOfHistoryRecordsがmaxNumberOfLinesHistoryCsvより大きいケース ===

        config.numberOfHistoryRecords = 9123456;
        List<Params> list =  generator.createParamsList(config.maxNumberOfLinesHistoryCsv);
        assertEquals(10, list.size());;

        // start～endが連続していることの確認
        assertEquals(config.historyMinDate.getTime(), list.get(0).start);
        for (int i = 0; i < 9; i++) {
            assertEquals(list.get(i).end, list.get(i+1).start, "i = " + i);
        }
        assertEquals(config.historyMaxDate.getTime(), list.get(list.size() - 1).end);

        // i == 0 のときをのぞき、sart-endの値が等しい
        for (int i = 1; i < list.size(); i++) {
            assertEquals(list.get(1).end - list.get(1).start, list.get(i).end - list.get(i).start, "i = " + i);
        }

        // i == 0 のときの start - endの値が i != 0 のときより小さく、その日がnumberOfHistoryの比と等しい
        long diff0 = list.get(0).end - list.get(0).start;
        long diff1 = list.get(1).end - list.get(1).start;
        assertTrue(diff0 <= diff1, "diff0 = " + diff0 + ", diff1 = " + diff1);
        assertEquals(list.get(1).numberOfHistory/(double) list.get(0).numberOfHistory,  diff1/(double)diff0, 1e-6);


        // 各paramsのnumberOfHistoryの合計が、config.numberOfHistoryRecordsと等しい
        assertEquals(config.numberOfHistoryRecords, list.parallelStream().mapToInt( p -> (int)p.numberOfHistory).sum());

        // i == 0のケースを除き、各paramsのnumberOfHistoryRecordsが、maxNumberOfLinesHistoryCsvと等しい
        for(int i = 1; i < list.size(); i++) {
            assertEquals(config.maxNumberOfLinesHistoryCsv, list.get(i).numberOfHistory, "i = " + i);
        }

        // taskIdのテスト
        for(int i = 0; i < list.size(); i++) {
            assertEquals(i, list.get(i).taskId);
        }


        // === numberOfHistoryRecordsがintで表現可能な値より大きいケース

        config.numberOfHistoryRecords = 10000000000L;
        config.numberOfContractsRecords = 1000000;
        config.maxNumberOfLinesHistoryCsv = 1000000000;
        list =  generator.createParamsList(config.maxNumberOfLinesHistoryCsv);
        assertEquals(config.numberOfHistoryRecords / config.maxNumberOfLinesHistoryCsv , list.size());;

        // start～endが連続していることの確認
        assertEquals(config.historyMinDate.getTime(), list.get(0).start);
        for (int i = 0; i < list.size() - 1; i++) {
            assertEquals(list.get(i).end, list.get(i+1).start, "i = " + i);
        }
        assertEquals(config.historyMaxDate.getTime(), list.get(list.size() - 1).end);

        // i == 0 のときをのぞき、sart-endの値が等しい
        for (int i = 1; i < list.size(); i++) {
            assertEquals(list.get(1).end - list.get(1).start, list.get(i).end - list.get(i).start, "i = " + i);
        }

        // i == 0 のときの start - endの値が i != 0 のときより小さく、その日がnumberOfHistoryの比と等しい
        diff0 = list.get(0).end - list.get(0).start;
        diff1 = list.get(1).end - list.get(1).start;
        assertTrue(diff0 <= diff1, "diff0 = " + diff0 + ", diff1 = " + diff1);
        assertEquals(list.get(1).numberOfHistory/(double) list.get(0).numberOfHistory,  diff1/(double)diff0, 1e-6);


        // 各paramsのnumberOfHistoryの合計が、config.numberOfHistoryRecordsと等しい
        assertEquals(config.numberOfHistoryRecords, list.parallelStream().mapToLong( p -> (int)p.numberOfHistory).sum());

        // i == 0のケースを除き、各paramsのnumberOfHistoryRecordsが、maxNumberOfLinesHistoryCsvと等しい
        for(int i = 1; i < list.size(); i++) {
            assertEquals(config.maxNumberOfLinesHistoryCsv, list.get(i).numberOfHistory, "i = " + i);
        }

        // taskIdのテスト
        for(int i = 0; i < list.size(); i++) {
            assertEquals(i, list.get(i).taskId);
        }


    }
}
