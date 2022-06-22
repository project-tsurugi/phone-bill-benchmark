package com.example.nedo.app.billing;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.AbstractDbTestCase;
import com.example.nedo.app.Config;
import com.example.nedo.app.Config.TransactionScope;
import com.example.nedo.db.old.Billing;
import com.example.nedo.db.old.Contract;
import com.example.nedo.db.old.DBUtils;
import com.example.nedo.db.old.Duration;
import com.example.nedo.db.old.History;
import com.example.nedo.db.old.History.Key;
import com.example.nedo.app.CreateTable;
import com.example.nedo.online.AbstractOnlineApp;
import com.example.nedo.online.HistoryInsertApp;
import com.example.nedo.online.HistoryUpdateApp;
import com.example.nedo.online.MasterInsertApp;
import com.example.nedo.online.MasterUpdateApp;
import com.example.nedo.testdata.AbstractContractBlockInfoInitializer;
import com.example.nedo.testdata.ContractBlockInfoAccessor;
import com.example.nedo.testdata.CreateTestData;
import com.example.nedo.testdata.DefaultContractBlockInfoInitializer;
import com.example.nedo.testdata.SingleProcessContractBlockManager;

class PhoneBillTest extends AbstractDbTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneBillTest.class);

	@Test
	void test() throws Exception {
		// 初期化
		CreateTable.main(new String[0]);
		PhoneBill phoneBill = new PhoneBill();
		phoneBill.config = Config.getConfig();

		// データが存在しない状態での料金計算
		phoneBill.doCalc(DBUtils.toDate("2020-11-01"), DBUtils.toDate("2020-11-30"));
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
		phoneBill.doCalc(DBUtils.toDate("2020-11-01"), DBUtils.toDate("2020-11-30"));
		List<Billing> billings = getBillings();
		assertEquals(5, billings.size());
		assertEquals(toBilling("Phone-0001", "2020-11-01", 3000, 0, 3000), billings.get(0));
		assertEquals(toBilling("Phone-0003", "2020-11-01", 3000, 0, 3000), billings.get(1));
		assertEquals(toBilling("Phone-0004", "2020-11-01", 3000, 0, 3000), billings.get(2));
		assertEquals(toBilling("Phone-0005", "2020-11-01", 3000, 0, 3000), billings.get(3));
		assertEquals(toBilling("Phone-0008", "2020-11-01", 3000, 0, 3000), billings.get(4));
		List<History> histories = getHistories();
		assertEquals(0, histories.size());


		// 通話履歴ありの場合
		insertToHistory("Phone-0001", "Phone-0008", "C", "2020-10-31 23:59:59.999", 30, false);		// 計算対象年月外
		insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:00:00.000", 30, false);  	// 計算対象
		insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-15 12:12:12.000", 90, true); 	 	// 削除フラグ
		insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-30 23:59:59.999", 90, false);  	// 計算対象
		insertToHistory("Phone-0001", "Phone-0008", "C", "2020-12-01 00:00:00.000", 30, false);  	// 計算対象年月外
		insertToHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 00:00:00.000", 250, false);  	// 計算対象
		insertToHistory("Phone-0005", "Phone-0008", "R", "2020-11-30 00:00:00.000", 30, false);  	// 計算対象(受信者負担)

		phoneBill.doCalc(DBUtils.toDate("2020-11-01"), DBUtils.toDate("2020-11-30"));
		billings = getBillings();
		assertEquals(5, billings.size());
		assertEquals(toBilling("Phone-0001", "2020-11-01", 3000, 30, 3000), billings.get(0));
		assertEquals(toBilling("Phone-0003", "2020-11-01", 3000, 0, 3000), billings.get(1));
		assertEquals(toBilling("Phone-0004", "2020-11-01", 3000, 0, 3000), billings.get(2));
		assertEquals(toBilling("Phone-0005", "2020-11-01", 3000, 50, 3000), billings.get(3));
		assertEquals(toBilling("Phone-0008", "2020-11-01", 3000, 10, 3000), billings.get(4));
		histories = getHistories();
		assertEquals(7, histories.size());
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-10-31 23:59:59.999", 30, null, false), histories.get(0));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:00:00.000", 30, 10, false), histories.get(1));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-15 12:12:12.000", 90, null, true), histories.get(2));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-30 23:59:59.999", 90, 20, false), histories.get(3));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-12-01 00:00:00.000", 30, null, false), histories.get(4));
		assertEquals(toHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 00:00:00.000", 250, 50, false), histories.get(5));
		assertEquals(toHistory("Phone-0005", "Phone-0008", "R", "2020-11-30 00:00:00.000", 30, 10, false), histories.get(6));


		// Exception 発生時にrollbackされることの確認
        // Phone-0001が先に処理されテーブルが更新されるが、Phone-005の処理でExceptionが発生し、処理全体がロールバックされる
		insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:30:00.000", 30, false);  	// 計算対象
		insertToHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 01:00:00.000", -1, false);  	// 通話時間が負数なのでExceptionがスローされる
		phoneBill.doCalc( DBUtils.toDate("2020-11-01"), DBUtils.toDate("2020-11-30"));
		billings = getBillings();
		assertEquals(5, billings.size());
		assertEquals(toBilling("Phone-0001", "2020-11-01", 3000, 30, 3000), billings.get(0));
		assertEquals(toBilling("Phone-0003", "2020-11-01", 3000, 0, 3000), billings.get(1));
		assertEquals(toBilling("Phone-0004", "2020-11-01", 3000, 0, 3000), billings.get(2));
		assertEquals(toBilling("Phone-0005", "2020-11-01", 3000, 50, 3000), billings.get(3));
		assertEquals(toBilling("Phone-0008", "2020-11-01", 3000, 10, 3000), billings.get(4));
		// 通話履歴も更新されていないことを確認
		histories = getHistories();
		assertEquals(9, histories.size());
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-10-31 23:59:59.999", 30, null, false), histories.get(0));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:00:00.000", 30, 10, false), histories.get(1));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:30:00.000", 30, null, false), histories.get(2));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-15 12:12:12.000", 90, null, true), histories.get(3));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-30 23:59:59.999", 90, 20, false), histories.get(4));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-12-01 00:00:00.000", 30, null, false), histories.get(5));
		assertEquals(toHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 00:00:00.000", 250, 50, false), histories.get(6));
		assertEquals(toHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 01:00:00.000", -1, null, false), histories.get(7));
		assertEquals(toHistory("Phone-0005", "Phone-0008", "R", "2020-11-30 00:00:00.000", 30, 10, false), histories.get(8));


		// Exceptionの原因となるレコードを削除して再実行
		String sql = "delete from history where caller_phone_number = 'Phone-0005' and start_time = '2020-11-10 01:00:00'";
		getStmt().execute(sql);
		phoneBill.doCalc( DBUtils.toDate("2020-11-01"), DBUtils.toDate("2020-11-30"));
		billings = getBillings();
		assertEquals(5, billings.size());
		assertEquals(toBilling("Phone-0001", "2020-11-01", 3000, 40, 3000), billings.get(0));
		assertEquals(toBilling("Phone-0003", "2020-11-01", 3000, 0, 3000), billings.get(1));
		assertEquals(toBilling("Phone-0004", "2020-11-01", 3000, 0, 3000), billings.get(2));
		assertEquals(toBilling("Phone-0005", "2020-11-01", 3000, 50, 3000), billings.get(3));
		assertEquals(toBilling("Phone-0008", "2020-11-01", 3000, 10, 3000), billings.get(4));
		histories = getHistories();
		assertEquals(8, histories.size());
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-10-31 23:59:59.999", 30, null, false), histories.get(0));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:00:00.000", 30, 10, false), histories.get(1));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:30:00.000", 30, 10, false), histories.get(2));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-15 12:12:12.000", 90, null, true), histories.get(3));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-30 23:59:59.999", 90, 20, false), histories.get(4));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-12-01 00:00:00.000", 30, null, false), histories.get(5));
		assertEquals(toHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 00:00:00.000", 250, 50, false), histories.get(6));
		assertEquals(toHistory("Phone-0005", "Phone-0008", "R", "2020-11-30 00:00:00.000", 30, 10, false), histories.get(7));



		// 論理削除フラグを立てたレコードが計算対象外になることの確認
		sql = "update history set df = 1 where caller_phone_number = 'Phone-0001' and start_time = '2020-11-30 23:59:59.999'";
		getStmt().execute(sql);
		phoneBill.doCalc( DBUtils.toDate("2020-11-01"), DBUtils.toDate("2020-11-30"));
		billings = getBillings();
		assertEquals(5, billings.size());
		assertEquals(toBilling("Phone-0001", "2020-11-01", 3000, 20, 3000), billings.get(0));
		assertEquals(toBilling("Phone-0003", "2020-11-01", 3000, 0, 3000), billings.get(1));
		assertEquals(toBilling("Phone-0004", "2020-11-01", 3000, 0, 3000), billings.get(2));
		assertEquals(toBilling("Phone-0005", "2020-11-01", 3000, 50, 3000), billings.get(3));
		assertEquals(toBilling("Phone-0008", "2020-11-01", 3000, 10, 3000), billings.get(4));
		histories = getHistories();
		assertEquals(8, histories.size());
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-10-31 23:59:59.999", 30, null, false), histories.get(0));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:00:00.000", 30, 10, false), histories.get(1));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:30:00.000", 30, 10, false), histories.get(2));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-15 12:12:12.000", 90, null, true), histories.get(3));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-30 23:59:59.999", 90, 20, true), histories.get(4));
		assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-12-01 00:00:00.000", 30, null, false), histories.get(5));
		assertEquals(toHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 00:00:00.000", 250, 50, false), histories.get(6));
		assertEquals(toHistory("Phone-0005", "Phone-0008", "R", "2020-11-30 00:00:00.000", 30, 10, false), histories.get(7));
	}


	private List<Billing> getBillings() throws SQLException {
		List<Billing> list = new ArrayList<Billing>();
		String sql = "select phone_number, target_month, basic_charge, metered_charge, billing_amount"
				+ " from billing order by phone_number, target_month";
		try (ResultSet rs = getStmt().executeQuery(sql)) {
			while (rs.next()) {
				Billing billing = new Billing();
				billing.phoneNumber = rs.getString(1);
				billing.targetMonth = rs.getDate(2);
				billing.basicCharge = rs.getInt(3);
				billing.meteredCharge = rs.getInt(4);
				billing.billingAmount = rs.getInt(5);
				list.add(billing);
			}
		}
		return list;
	}


	private Billing toBilling(String phoneNumber, String targetMonth, int basicCharge, int meteredCharge,
			int billingAmount) {
		Billing billing = new Billing();
		billing.phoneNumber = phoneNumber;
		billing.targetMonth = DBUtils.toDate(targetMonth);
		billing.basicCharge = basicCharge;
		billing.meteredCharge = meteredCharge;
		billing.billingAmount = billingAmount;
		return billing;
	}

	/**
	 * 契約マスタにレコードを追加する
	 *
	 * @param phoneNumber
	 * @param startDate
	 * @param endDate
	 * @param chargeRule
	 * @throws SQLException
	 */
	private void insertToContracts(String phoneNumber, String startDate, String endDate, String chargeRule)
			throws SQLException {
		String sql = "insert into contracts(phone_number, start_date, end_date, charge_rule) values(?, ?, ?, ?)";
		try (PreparedStatement ps = getConn().prepareStatement(sql)) {
			ps.setString(1, phoneNumber);
			ps.setDate(2, DBUtils.toDate(startDate));
			if (endDate == null) {
				ps.setNull(3, Types.DATE);
			} else {
				ps.setDate(3, DBUtils.toDate(endDate));
			}
			ps.setString(4, chargeRule);
			int c = ps.executeUpdate();
			assertEquals(1, c);
		}
	}

	/**
	 * 履歴テーブルにレコードを追加する
	 *
	 * @param caller_phone_number 発信者電話番号
	 * @param recipient_phone_number 受信者電話番号
	 * @param payment_categorty	料金区分
	 * @param start_time 通話開始時刻
	 * @param time_secs 通話時間
	 * @param df 論理削除フラグ
	 * @throws SQLException
	 */
	private void insertToHistory(String caller_phone_number, String recipient_phone_number, String payment_categorty, String start_time, Integer time_secs, boolean df)
			throws SQLException {
		String sql = "insert into history(caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs, charge, df) values(?, ?, ?, ?, ?, ?, ?)";
		try (PreparedStatement ps = getConn().prepareStatement(sql)) {
			ps.setString(1, caller_phone_number);
			ps.setString(2, recipient_phone_number);
			ps.setString(3, payment_categorty);
			ps.setTimestamp(4, DBUtils.toTimestamp(start_time));
			ps.setInt(5, time_secs);
			ps.setNull(6, Types.INTEGER);
			ps.setInt(7, df ? 1 : 0);
			int c = ps.executeUpdate();
			assertEquals(1, c);
		}
	}


	/**
	 * toDuration()のテスト
	 */
	@Test
	void testToDuration() {
		Duration d;

		d = PhoneBill.toDuration(DBUtils.toDate("2020-12-01"));
		assertEquals(DBUtils.toDate("2020-12-01"), d.getStatDate());
		assertEquals(DBUtils.toDate("2020-12-31"), d.getEndDate());

		d = PhoneBill.toDuration(DBUtils.toDate("2020-12-31"));
		assertEquals(DBUtils.toDate("2020-12-01"), d.getStatDate());
		assertEquals(DBUtils.toDate("2020-12-31"), d.getEndDate());

		d = PhoneBill.toDuration(DBUtils.toDate("2021-02-05"));
		assertEquals(DBUtils.toDate("2021-02-01"), d.getStatDate());
		assertEquals(DBUtils.toDate("2021-02-28"), d.getEndDate());

	}

	/*
	 * Configに違いがあっても処理結果が変わらないことを確認
	 */
	@Test
	void testConfigVariation() throws Exception {
		// まず実行し、その結果を期待値とする
		Config config = Config.getConfig();
		config.duplicatePhoneNumberRate = 10;
		config.expirationDateRate = 10;
		config.noExpirationDateRate = 70;
		config.numberOfContractsRecords = (int) 100;
		config.numberOfHistoryRecords = (int) 1000;
		config.threadCount = 1;
		config.sharedConnection = false;
		new CreateTable().execute(config);
		new CreateTestData().execute(config);
		PhoneBill phoneBill = new PhoneBill();
		phoneBill.execute(config);
		List<Billing> expected = getBillings();

		// スレッド数 、コネクションプールの有無で結果が変わらないことを確認
		boolean[] sharedConnections = { false, true };
		int[] threadCounts = { 1, 2, 4, 8, 16 };
		for (boolean sharedConnection : sharedConnections) {
			for (int threadCount : threadCounts) {
				Config newConfig = config.clone();
				newConfig.threadCount = threadCount;
				newConfig.sharedConnection = sharedConnection;
				LOG.info("Executing phoneBill.exec() with threadCount =" + threadCount +
						", sharedConnection = " + sharedConnection);
				testNewConfig(phoneBill, expected, newConfig);
			}
		}

		// トランザクションスコープの違いで結果が変わらないことの確認
		for(TransactionScope ts: TransactionScope.values()) {
			Config newConfig = config.clone();
			newConfig.transactionScope= ts;
			LOG.info("Executing phoneBill.exec() with transactionScope =" + ts);
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
		config.masterInsertReccrdsPerMin = 0;
		config.masterUpdateRecordsPerMin = 0;
		config.historyInsertThreadCount = 1;
		config.historyUpdateThreadCount = 1;
		config.masterInsertThreadCount = 1;
		config.masterUpdateThreadCount = 1;
		assertEquals(Collections.emptyList(), PhoneBill.createOnlineApps(config, accessor));

		// オンラインアプリを動かさないケース(スレッド数の指定が0)
		config.historyInsertTransactionPerMin = 1;
		config.historyUpdateRecordsPerMin = 1;
		config.masterInsertReccrdsPerMin = 1;
		config.masterUpdateRecordsPerMin = 1;
		config.historyInsertThreadCount = 0;
		config.historyUpdateThreadCount = 0;
		config.masterInsertThreadCount = 0;
		config.masterUpdateThreadCount = 0;
		assertEquals(Collections.emptyList(), PhoneBill.createOnlineApps(config, accessor));

		// スレッド数で指定された数だけ、オンラインアプリが作成されていることを確認する
		config.historyInsertTransactionPerMin = 1;
		config.historyUpdateRecordsPerMin = 1;
		config.masterInsertReccrdsPerMin = 1;
		config.masterUpdateRecordsPerMin = 1;
		config.historyInsertThreadCount = 1;
		config.historyUpdateThreadCount = 2;
		config.masterInsertThreadCount = 3;
		config.masterUpdateThreadCount = 4;

		// オンラインアプリがconfigで指定された数だけ作成されることの確認
		Map<Class<?>, List<AbstractOnlineApp>> map = new HashMap<>();
		map.put(HistoryInsertApp.class, new ArrayList<AbstractOnlineApp>());
		map.put(HistoryUpdateApp.class, new ArrayList<AbstractOnlineApp>());
		map.put(MasterInsertApp.class, new ArrayList<AbstractOnlineApp>());
		map.put(MasterUpdateApp.class, new ArrayList<AbstractOnlineApp>());
		for(AbstractOnlineApp app: PhoneBill.createOnlineApps(config, accessor)) {
			List<AbstractOnlineApp>	list = map.get(app.getClass());
			list.add(app);
		}

		// 生成されたオンラインアプリの数とnameの確認
		checkAppList(1, "HistoryInsertApp", map.get(HistoryInsertApp.class));
		checkAppList(2, "HistoryUpdateApp", map.get(HistoryUpdateApp.class));
		checkAppList(3, "MasterInsertApp", map.get(MasterInsertApp.class));
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

		String[] args = {"src/test/config/phone_bill_test.properties"};
		PhoneBill.main(args);

		List<History> historiesAfter = getHistories();
		List<Contract> contractsAfter = getContracts();

		// オンラインアプリによりレコード数が増えていることを確認
		assertTrue(historiesAfter.size() > historiesBefore.size());
		assertTrue(contractsAfter.size() > historiesBefore.size());
		System.out.println("historiesBeforeSize = " + historiesBefore.size());
		System.out.println("historiesAfterSize = " + historiesAfter.size());
		System.out.println("contractsBeforeSize = " + contractsBefore.size());
		System.out.println("contractsAfterSize = " + contractsAfter.size());


		// オンラインアプリにより契約終了日が削除されているレコードが存在することを確認
		boolean exist = false;
		for (int i = 0; i < contractsBefore.size(); i++) {
			Contract before = contractsBefore.get(i);
			Contract after = contractsAfter.get(i);
			if (before.endDate == null && after.endDate != null) {
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
			if (before.endDate != null && after.endDate != null && !before.endDate.equals(after.endDate)) {
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
			if (before.df == false && after.df == true) {
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
			if (before.timeSecs != after.timeSecs) {
				exist = true;
				System.out.println("before = " + before);
				System.out.println("after  = " + after);
				break;
			}
		}
		assertTrue(exist);
	}
}

