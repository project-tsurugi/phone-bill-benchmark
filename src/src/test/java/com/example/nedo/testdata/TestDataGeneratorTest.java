/**
 *
 */
package com.example.nedo.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.example.nedo.AbstractDbTestCase;
import com.example.nedo.app.Config;
import com.example.nedo.app.CreateTable;
import com.example.nedo.db.DBUtils;
import com.example.nedo.db.Duration;
import com.example.nedo.testdata.GenerateHistoryTask.Params;
import com.example.nedo.util.PathUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

class TestDataGeneratorTest extends AbstractDbTestCase {
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
		return TestDataGenerator.isValidDurationList(list, DBUtils.toDate(start), DBUtils.toDate(end));
	}


	/**
	 * generateContractToDb()のテスト
	 * @throws Exception
	 */
	@Test
	void testGenerateContractToDb() throws Exception {
		new CreateTable().execute(Config.getConfig());

		Config config = Config.getConfig();
		config.minDate = DBUtils.toDate("2010-01-11");
		config.maxDate = DBUtils.toDate("2020-12-21");
		config.numberOfContractsRecords = 10000;
		config.expirationDateRate =5;
		config.noExpirationDateRate = 11;
		config.duplicatePhoneNumberRatio = 2;

		TestDataGenerator generator = new TestDataGenerator(config);
		generator.generateContractsToDb();

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
	 * initDurationList()のテスト
	 * @throws IOException
	 */
	@Test
	void testInitDurationList() throws IOException {
		// 通常ケース
		testInitDurationLisSubt(1, 3, 7, DBUtils.toDate("2010-11-11"), DBUtils.toDate("2020-01-01"));
		testInitDurationLisSubt(13, 5, 2, DBUtils.toDate("2010-11-11"), DBUtils.toDate("2020-01-01"));
		// 一項目が0
		testInitDurationLisSubt(3, 7, 0, DBUtils.toDate("2010-11-11"), DBUtils.toDate("2020-01-01"));
		testInitDurationLisSubt(3, 0, 5, DBUtils.toDate("2010-11-11"), DBUtils.toDate("2020-01-01"));
		testInitDurationLisSubt(0, 7, 5, DBUtils.toDate("2010-11-11"), DBUtils.toDate("2020-01-01"));
		// 二項目が0
		testInitDurationLisSubt(0, 7, 0, DBUtils.toDate("2010-11-11"), DBUtils.toDate("2020-01-01"));
		testInitDurationLisSubt(3, 0, 0, DBUtils.toDate("2010-11-11"), DBUtils.toDate("2020-01-01"));
		testInitDurationLisSubt(0, 0, 5, DBUtils.toDate("2010-11-11"), DBUtils.toDate("2020-01-01"));
		// startの翌日=endのケース
		testInitDurationLisSubt(13, 5, 2, DBUtils.toDate("2019-12-31"), DBUtils.toDate("2020-01-01"));

	}

	void testInitDurationLisSubt(int duplicatePhoneNumberRatio, int expirationDateRate, int noExpirationDateRate
		, Date start, Date end) throws IOException {
		Config config = Config.getConfig();
		config.duplicatePhoneNumberRatio = duplicatePhoneNumberRatio;
		config.expirationDateRate = expirationDateRate;
		config.noExpirationDateRate = noExpirationDateRate;
		config.minDate = start;
		config.maxDate = end;


		TestDataGenerator generator = new TestDataGenerator(config);
		List<Duration> list = generator.getDurationList();
		// listの要素数が duplicatePhoneNumberRatio * 2 + expirationDateRate + noExpirationDateRateであること
		assertEquals(duplicatePhoneNumberRatio * 2 + expirationDateRate + noExpirationDateRate, list.size());
		// 始めの、expirationDateRate + noExpirationDateRate 個の要素を調べると、契約終了日が存在する要素数が
		// expirationDateRate, 契約終了日が存在しない要数がnoExpirationDateRateであること。
		int n1 = 0;
		int n2 = 0;
		for(int i = 0; i < expirationDateRate + noExpirationDateRate; i++) {
			Duration d = list.get(i);
			assertNotNull(d.start);
			if (d.end == null) {
				n1++;
			} else {
				n2++;
			}
		}
		assertEquals(noExpirationDateRate, n1);
		assertEquals(expirationDateRate, n2);
		// expirationDateRate + noExpirationDateRateより後の要素は以下の2つの要素のペアが、duplicatePhoneNumberRatio個
		// 続いていること。
		//
		// 1番目の要素の、startがContractsGeneratorのコンストラクタに渡したstartと等しい
		// 2番目の要素のendがnull
		// 2番目の要素のstartが1番目の要素のendより大きい

		for(int i = expirationDateRate + noExpirationDateRate; i < list.size(); i+=2) {
			Duration d1 = list.get(i);
			Duration d2 = list.get(i+1);
			assertEquals(start, d1.getStatDate());
			assertTrue(d1.end < d2.start);
			assertNull(d2.end);
		}
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
		return new Duration(DBUtils.toDate(start), DBUtils.toDate(end));
	}



	/**
	 * getPhoneNumber()のテスト
	 * @throws IOException
	 */
	@Test
	void testGetDuration() throws IOException {
		Config config = Config.getConfig();
		config.duplicatePhoneNumberRatio = 2;
		config.expirationDateRate = 3;
		config.noExpirationDateRate = 4;

		TestDataGenerator generator = new TestDataGenerator(config);
		List<Duration> list = generator.getDurationList();
		for (int i = 0; i < 20; i++) {
			Duration expected = list.get(i % (2 * 2 + 3 + 4));
			Duration actual = generator.getDuration(i);
			assertEquals(expected, actual);
		}

	}

	/**
	 * getDate()で得られる値が、start ～ endの範囲に収まることのテスト
	 * @throws IOException
	 */
	@Test
	void tesGetDate3() throws IOException {
		Date start = DBUtils.toDate("2020-11-30");
		Date end = DBUtils.toDate("2020-12-02");
		Set<Date> expected = new TreeSet<>(Arrays.asList(
				DBUtils.toDate("2020-11-30"),
				DBUtils.toDate("2020-12-01"),
				DBUtils.toDate("2020-12-02")));
		Set<Date> actual = new TreeSet<>();

		Config config = Config.getConfig();
		config.minDate = start;
		config.maxDate = end;
		TestDataGenerator generator = new TestDataGenerator(config);
		for(int i = 0; i < 100; i++) {
			actual.add(generator.getDate(start, end));
		}
		assertEquals(expected, actual);
	}

	/**
	 * getDate()で得られる値が、start ～ endの範囲に収まることのテスト
	 * @throws IOException
	 */
	@Test
	void tesGetDate7() throws IOException {
		Date start = DBUtils.toDate("2020-11-30");
		Date end = DBUtils.toDate("2020-12-06");
		Set<Date> expected = new TreeSet<>(Arrays.asList(
				DBUtils.toDate("2020-11-30"),
				DBUtils.toDate("2020-12-01"),
				DBUtils.toDate("2020-12-02"),
				DBUtils.toDate("2020-12-03"),
				DBUtils.toDate("2020-12-04"),
				DBUtils.toDate("2020-12-05"),
				DBUtils.toDate("2020-12-06")));
		Set<Date> actual = new TreeSet<>();

		Config config = Config.getConfig();
		config.minDate = start;
		config.maxDate = end;
		TestDataGenerator generator = new TestDataGenerator(config);
		for(int i = 0; i < 100; i++) {
			actual.add(generator.getDate(start, end));
		}
		assertEquals(expected, actual);
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
		new CreateTable().execute(config);
		getStmt().execute("truncate table contracts");
		new TestDataGenerator(config).generateContractsToDb();
		Path expectedFilePath = tempDir.resolve("contracts.db").toAbsolutePath();
		String expectedFilePathString = PathUtils.toWls(expectedFilePath);

		getStmt().execute("copy contracts to '"+expectedFilePathString+"' with csv");
		List<String> expected = Files.readAllLines(expectedFilePath);
		Collections.sort(expected);

		Path contracts = CsvUtils.getContractsFilePath(tempDir);
		new TestDataGenerator(config).generateContractsToCsv(contracts);
		List<String> actual = Files.readAllLines(contracts);
		Collections.sort(actual);

		assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); i++) {
			assertEquals(expected.get(i), actual.get(i));
		}
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
		new TestDataGenerator(config).generateHistoryToDb();
		Path expectedFilePath = tempDir.resolve("history.db").toAbsolutePath();
		String expectedFilePathString = PathUtils.toWls(expectedFilePath);

		getStmt().execute("copy history to '"+expectedFilePathString+"' with csv");
		List<String> expected = Files.readAllLines(expectedFilePath);
		Collections.sort(expected);

		new TestDataGenerator(config).generateHistoryToCsv(tempDir);
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
		TestDataGenerator generator = new TestDataGenerator(config);
		int numberOfParams = 0;
		for(Params params: generator.createParamsList()) {
			numberOfParams++;
			assertEquals(config.historyMinDate.getTime(), params.start);
			assertEquals(config.historyMaxDate.getTime(), params.end);
			assertEquals(config.numberOfHistoryRecords, params.numberOfHistory);
		}
		assertEquals(1, numberOfParams);

		// === numberOfHistoryRecordsがmaxNumberOfLinesHistoryCsvより大きいケース ===

		config.numberOfHistoryRecords = 9123456;
		List<Params> list =  generator.createParamsList();
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
		list =  generator.createParamsList();
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
