package com.example.nedo.online;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.example.nedo.AbstractDbTestCase;
import com.example.nedo.app.Config;
import com.example.nedo.app.CreateTable;
import com.example.nedo.db.Contract;
import com.example.nedo.db.Contract.Key;
import com.example.nedo.db.DBUtils;
import com.example.nedo.db.History;
import com.example.nedo.online.HistoryUpdateApp.Updater;
import com.example.nedo.testdata.CreateTestData;

class HistoryUpdateAppTest extends AbstractDbTestCase {
	private Config config;
	private HistoryUpdateApp app;


	@BeforeEach
	void before() throws Exception {
		// テストデータを入れる
		config = Config.getConfig();
		new CreateTable().execute(config);
		new CreateTestData().execute(config);

		// アプリケーションの初期化
		ContractHolder contractHolder = new ContractHolder(config);
		app = new HistoryUpdateApp(contractHolder, config, 0);
	}

	@AfterEach
	void after() throws SQLException {
		app.getConnection().rollback();
	}


	@Test
	void testExec() throws Exception {
		List<History> expected = getHistories();
		testExecSub(expected);

		app.exec();
		app.getConnection().commit();
		expected.get(952).timeSecs = 1116;
		testExecSub(expected);

		app.exec();
		app.getConnection().commit();
		expected.get(495).timeSecs = 2055;
		testExecSub(expected);

		app.exec();
		app.getConnection().commit();
		expected.get(484).timeSecs = 2445;
		testExecSub(expected);

		app.exec();
		app.getConnection().commit();
		expected.get(869).df = true;
		testExecSub(expected);

		app.exec();
		app.getConnection().commit();
		expected.get(225).timeSecs = 438;
		testExecSub(expected);

		app.exec();
		app.getConnection().commit();
		expected.get(601).df = true;
		testExecSub(expected);
	}

	private void testExecSub(List<History> expected) throws SQLException {
		List<History> actual = getHistories();
		assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); i++) {
			assertEquals(expected.get(i), actual.get(i), "i = " + i);
		}
	}


	/**
	 * getHistories()のテスト
	 */
	@Test
	void testGetHistories() throws Exception {
		Key key;
		Set<History> actual;
		Set<History> expected = new HashSet<History>();

		// 同一電話番号のレコードが1レコード、end_dateがnullの契約
		key = Contract.createKey("00000000015", DBUtils.toDate("2013-12-09"));
		actual = new HashSet<History>(app.getHistories(key));
		expected.clear();
		expected.add(toHistory("00000000015", "00000000165", "R", "2020-12-17 18:15:40.035", 2982, null, false));
		expected.add(toHistory("00000000015", "00000000832", "R", "2021-01-09 12:08:33.395", 3054, null, false));
		expected.add(toHistory("00000000015", "00000000385", "R", "2020-12-29 17:34:09.622", 1277, null, false));
		assertEquals(expected, actual);

		// 同一電話番号のレコードが1レコード、end_dateがnullでない契約
		key = Contract.createKey("00000000375", DBUtils.toDate("2017-03-11"));
		actual = new HashSet<History>(app.getHistories(key));
		expected.clear();
		expected.add(toHistory("00000000375", "00000000865", "C", "2020-11-08 05:54:24.471", 3078, null, false));
		expected.add(toHistory("00000000375", "00000000565", "C", "2020-11-29 18:41:55.993", 2876, null, false));
		assertEquals(expected, actual);

		// 同一電話番号のレコードが2レコードの契約
		key = Contract.createKey("00000000391", DBUtils.toDate("2010-11-11"));
		actual = new HashSet<History>(app.getHistories(key));
		expected.clear();
		assertEquals(expected, actual);

		key = Contract.createKey("00000000391", DBUtils.toDate("2014-10-16"));
		actual = new HashSet<History>(app.getHistories(key));
		expected.clear();
		expected.add(toHistory("00000000391", "00000000265", "R", "2020-12-27 03:59:18.747", 2492, null, false));
		expected.add(toHistory("00000000391", "00000000273", "R", "2021-01-04 06:46:59.250", 951, null, false));
		expected.add(toHistory("00000000391", "00000000626", "R", "2020-11-24 20:33:31.285", 2978, null, false));
		assertEquals(expected, actual);

		// 二つの契約両方にマッチする履歴があるケース
		executeSql("update history set start_time = to_timestamp('2011-11-12 06:25:57.430','YYYY-MM-DD HH24:MI:SS.MS')"
				+ " where caller_phone_number = '00000000391'"
				+ " and start_time = to_timestamp('2020-12-27 03:59:18.747','YYYY-MM-DD HH24:MI:SS.MS')");

		key = Contract.createKey("00000000391", DBUtils.toDate("2010-11-11"));
		actual = new HashSet<History>(app.getHistories(key));
		expected.clear();
		expected.add(toHistory("00000000391", "00000000265", "R", "2011-11-12 06:25:57.430", 2492, null, false));
		assertEquals(expected, actual);

		key = Contract.createKey("00000000391", DBUtils.toDate("2014-10-16"));
		actual = new HashSet<History>(app.getHistories(key));
		expected.clear();
		expected.add(toHistory("00000000391", "00000000273", "R", "2021-01-04 06:46:59.250", 951, null, false));
		expected.add(toHistory("00000000391", "00000000626", "R", "2020-11-24 20:33:31.285", 2978, null, false));
		assertEquals(expected, actual);
	}


	/**
	 * updateDatabase()のテスト
	 */
	@Test
	void testUpdateDatabase() throws Exception {
		// 指定のキーで通話履歴を検索した値が想定通りであることの確認
		Key key = Contract.createKey("00000000391", DBUtils.toDate("2014-10-16"));
		HashSet<History> actual;
		Set<History> expected = new HashSet<History>();
		expected.add(toHistory("00000000391", "00000000265", "R", "2020-12-27 03:59:18.747", 2492, null, false));
		expected.add(toHistory("00000000391", "00000000273", "R", "2021-01-04 06:46:59.250", 951, null, false));
		expected.add(toHistory("00000000391", "00000000626", "R", "2020-11-24 20:33:31.285", 2978, null, false));
		actual = new HashSet<History>(app.getHistories(key));
		assertEquals(expected, actual);

		// 更新する履歴データ
		History history = toHistory("00000000391", "00000000273", "R", "2021-01-04 06:46:59.250", 951, null, false);

		// 更新されていることの確認
		expected.remove(history);
		history.recipientPhoneNumber = "RECV";
		history.charge=999;
		history.df = true;
		history.paymentCategorty = "C";
		history.timeSecs = 221;
		expected.add(history);
		app.updateDatabase(history);
		app.getConnection().commit();
		actual = new HashSet<History>(app.getHistories(key));
		assertEquals(expected, actual);
	}


	/**
	 * Updater1のテスト
	 *
	 * @throws SQLException
	 * @throws IOException
	 */
	@Test
	void testUpdater1() throws SQLException, IOException {
		Config config = Config.getConfig();
		Updater updater = new HistoryUpdateApp(null, config, 0).new Updater1();

		History history = toHistory("00000000391", "00000000105", "R", "2020-11-02 06:25:57.430", 1688, null, false);
		History expected = history.clone();
		updater.update(history);

		// 削除フラグが立っていることを確認
		expected.df = true;
		assertEquals(expected, history);
		// 2回呼び出しても変化ないことを確認
		updater.update(history);
		assertEquals(expected, history);
	}


	/**
	 * Updater2のテスト
	 *
	 * @throws SQLException
	 * @throws IOException
	 */
	@Test
	void testUpdater2() throws SQLException, IOException {
		Config config = Config.getConfig();
		Updater updater = new HistoryUpdateApp(null, config, 0).new Updater2();

		History history = toHistory("00000000391", "00000000105", "R", "2020-11-02 06:25:57.430", 1688, null, false);
		History expected = history.clone();

		// 通話時間が変わっていることを確認
		updater.update(history);
		expected.timeSecs = 961;
		assertEquals(expected, history);

		// 通話時間が変わっていることを確認
		updater.update(history);
		expected.timeSecs = 3149;
		assertEquals(expected, history);
	}
}
