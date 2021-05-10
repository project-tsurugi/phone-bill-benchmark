package com.example.nedo.online;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.example.nedo.AbstractDbTestCase;
import com.example.nedo.app.Config;
import com.example.nedo.app.CreateTable;
import com.example.nedo.db.DBUtils;
import com.example.nedo.db.History;
import com.example.nedo.online.ContractKeyHolder.Key;
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
		Random random = new Random(0);
		ContractKeyHolder contractKeyHolder = new ContractKeyHolder(config);
		app = new HistoryUpdateApp(contractKeyHolder, config, random);
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
		expected.get(496).timeSecs = 2055;
		testExecSub(expected);

		app.exec();
		app.getConnection().commit();
		expected.get(484).timeSecs = 2445;
		testExecSub(expected);

		app.exec();
		app.getConnection().commit();
		expected.get(871).df = true;
		testExecSub(expected);

		app.exec();
		app.getConnection().commit();
		expected.get(526).df = true;
		testExecSub(expected);

		app.exec();
		app.getConnection().commit();
		expected.get(508).timeSecs = 3024;
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
		key = ContractKeyHolder.createKey("00000000910", DBUtils.toDate("2019-10-28"));
		actual = new HashSet<History>(app.getHistories(key));
		expected.clear();
		expected.add(toHistory("00000000910", "00000000081", "R", "2020-11-25 07:32:43.843", 2354, null, false));
		expected.add(toHistory("00000000910", "00000000173", "C", "2020-11-28 05:35:33.173", 1873, null, false));
		expected.add(toHistory("00000000910", "00000000026", "C", "2020-12-22 09:06:34.589", 1925, null, false));
		assertEquals(expected, actual);

		// 同一電話番号のレコードが1レコード、end_dateがnullでない契約
		key = ContractKeyHolder.createKey("00000000375", DBUtils.toDate("2017-03-11"));
		actual = new HashSet<History>(app.getHistories(key));
		expected.clear();
		expected.add(toHistory("00000000375", "00000000912", "R", "2020-11-16 05:45:58.517", 1384, null, false));
		expected.add(toHistory("00000000375", "00000000865", "C", "2020-11-08 05:54:24.471", 3078, null, false));
		expected.add(toHistory("00000000375", "00000000565", "C", "2020-11-29 18:41:55.992", 2876, null, false));
		assertEquals(expected, actual);

		// 同一電話番号のレコードが2レコードの契約
		key = ContractKeyHolder.createKey("00000000391", DBUtils.toDate("2010-11-11"));
		actual = new HashSet<History>(app.getHistories(key));
		expected.clear();
		assertEquals(expected, actual);

		key = ContractKeyHolder.createKey("00000000391", DBUtils.toDate("2014-10-16"));
		actual = new HashSet<History>(app.getHistories(key));
		expected.clear();
		expected.add(toHistory("00000000391", "00000000265", "R", "2020-12-27 03:59:18.746", 2492, null, false));
		expected.add(toHistory("00000000391", "00000000273", "R", "2021-01-04 06:46:59.250", 951, null, false));
		assertEquals(expected, actual);

		// 二つの契約両方にマッチする履歴があるケース
		executeSql("update history set start_time = to_timestamp('2011-11-12 06:25:57.430','YYYY-MM-DD HH24:MI:SS.MS')"
				+ " where caller_phone_number = '00000000391'"
				+ " and start_time = to_timestamp('2020-12-27 03:59:18.746','YYYY-MM-DD HH24:MI:SS.MS')");

		key = ContractKeyHolder.createKey("00000000391", DBUtils.toDate("2010-11-11"));
		actual = new HashSet<History>(app.getHistories(key));
		expected.clear();
		expected.add(toHistory("00000000391", "00000000265", "R", "2011-11-12 06:25:57.430", 2492, null, false));
		assertEquals(expected, actual);

		key = ContractKeyHolder.createKey("00000000391", DBUtils.toDate("2014-10-16"));
		actual = new HashSet<History>(app.getHistories(key));
		expected.clear();
		expected.add(toHistory("00000000391", "00000000273", "R", "2021-01-04 06:46:59.250", 951, null, false));
		assertEquals(expected, actual);
	}


	/**
	 * updateDatabase()のテスト
	 */
	@Test
	void testUpdateDatabase() throws Exception {
		// 指定のキーで通話履歴を検索した値が想定通りであることの確認
		Key key = ContractKeyHolder.createKey("00000000391", DBUtils.toDate("2014-10-16"));
		HashSet<History> actual;
		Set<History> expected = new HashSet<History>();
		expected.add(toHistory("00000000391", "00000000265", "R", "2020-12-27 03:59:18.746", 2492, null, false));
		expected.add(toHistory("00000000391", "00000000273", "R", "2021-01-04 06:46:59.250", 951, null, false));
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
		Random random = new Random();
		Updater updater = new HistoryUpdateApp(null, config, random).new Updater1();

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
		Random random = new Random(0);
		Updater updater = new HistoryUpdateApp(null, config, random).new Updater2();

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
