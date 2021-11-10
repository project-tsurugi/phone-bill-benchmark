package com.example.nedo.online;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

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
import com.example.nedo.testdata.AbstractContractBlockInfoInitializer;
import com.example.nedo.testdata.ContractBlockInfoAccessor;
import com.example.nedo.testdata.CreateTestData;
import com.example.nedo.testdata.DefaultContractBlockInfoInitializer;
import com.example.nedo.testdata.SingleProcessContractBlockManager;
import com.example.nedo.util.RandomStub;

class HistoryUpdateAppTest extends AbstractDbTestCase {
	private Config config;
	private HistoryUpdateApp app;
	private RandomStub random;
	private ContractBlockInfoAccessor accessor;

	@BeforeEach
	void before() throws Exception {
		// テストデータを入れる
		config = Config.getConfig();
		config.numberOfContractsRecords = 10;
		config.expirationDateRate =3;
		config.noExpirationDateRate = 3;
		config.duplicatePhoneNumberRate = 2;
		config.numberOfHistoryRecords = 1000;

		new CreateTable().execute(config);
		new CreateTestData().execute(config);

		// アプリケーションの初期化
		AbstractContractBlockInfoInitializer initializer = new DefaultContractBlockInfoInitializer(config);
		accessor = new SingleProcessContractBlockManager(initializer);
		random = new RandomStub();
		app = new HistoryUpdateApp(config, random, accessor);
	}

	@AfterEach
	void after() throws SQLException {
		app.getConnection().rollback();
	}


	@Test
	void testExec() throws Exception {
		 List<History> histories = getHistories();
		 List<Contract> contracts = getContracts();
		Map<Key, List<History>> map = getContractHistoryMap(contracts, histories);

		// 削除フラグを立てるケース
		History target;
		target = histories.get(48);
		setRandom(contracts, map, target, true, 0);
		app.exec();
		app.getConnection().commit();
		target.df = true;
		target.charge = null;
		testExecSub(histories);

		// 通話時間を更新するケース
		target = histories.get(48);
		setRandom(contracts, map, target, false, 3185);
		app.exec();
		app.getConnection().commit();
		target.timeSecs = 3185 +1; // 通話時間は random.next() + 1 なので、
		target.charge = null;
		testExecSub(histories);


	}

	/**
	 * 指定したtargetを対象に履歴が更新されるように乱数生成器のスタブを設定する
	 *
	 * @param contracts 契約マスタ
	 * @param map 契約マスタと当該契約に属する履歴のリストのマップ
	 * @param target 更新対象の履歴
	 * @param delete trueのとき論理削除、falseの時、通話時間を更新する。
	 * @param timeSec 通話時間を更新するときの通話時間
	 */
	private void setRandom(List<Contract> contracts, Map<Key, List<History>> map, History target, boolean delete, int timeSec) {
		int nContract = -1;
		int nHistory = -1;
		for (int i = 0; i < contracts.size(); i++) {
			Key key = contracts.get(i).getKey();
			List<History> list = map.get(key);
			for (int j = 0; j < list.size(); j++) {
				if (list.get(j) == target) {
					nContract = i;
					nHistory = j;
					break;
				}
			}
			if (nContract >= 0) {
				break;
			}
		}
		if (delete) {
			random.setValues(0, nContract, nHistory, 0);
		} else {
//			random.setValues(0, nContract, nHistory, 1);
			random.setValues(0, nContract, nHistory, 1, timeSec);
		}
	}



	private void testExecSub(List<History> expected) throws SQLException, IOException {
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
		// chargeがnullでない履歴を作る
		History h = getHistories().get(0);
		h.charge = 200;
		app.updateDatabase(h);
		app.getConnection().commit();
		List<History> histories = getHistories();
		List<Contract> contracts = getContracts();

		 Map<Key, List<History>> map = getContractHistoryMap(contracts, histories);


		// すべてのキーについて、getHistory()の値が期待通りかを確認する
		for(Entry<Key, List<History>> entry: map.entrySet()) {
			assertEquals(entry.getValue(), app.getHistories(entry.getKey()));
		}

	}


	/**
	 * すべての契約と契約に属する履歴のマップを作成する
	 *
	 * @return
	 * @throws SQLException
	 */
	Map<Key, List<History>> getContractHistoryMap(List<Contract> contracts, List<History> histories) throws SQLException {
		// 通話履歴の開始時刻との比較を簡単にするため、契約のendDateを書き換える
		contracts.stream().forEach( c -> c.endDate =  c.endDate == null ? DBUtils.toDate("2099-12-31") : DBUtils.nextDate(c.endDate));
		Map<Key, List<History>> map = new HashMap<>();
		for (Contract c : contracts) {
			List<History> list = new ArrayList<History>();
			for (History h : histories) {
				if (h.callerPhoneNumber.equals(c.phoneNumber) &&
						c.startDate.getTime() <= h.startTime.getTime() &&
						h.startTime.getTime() < c.endDate.getTime()) {
					list.add(h);
				}
			}
			map.put(c.getKey(), list);
		}
		// すべての履歴データが一致するマスタを持つことを確認
		assertEquals(histories.size(), map.values().stream().mapToInt(s -> s.size()).sum());

		return map;
	}

	/**
	 * updateDatabase()のテスト
	 */
	@Test
	void testUpdateDatabase() throws Exception {
		List<History> expected = getHistories();

		// 最初のレコードを書き換える
		{
			History history = expected.get(0);
			history.recipientPhoneNumber = "RECV";
			history.charge = 999;
			history.df = true;
			history.paymentCategorty = "C";
			history.timeSecs = 221;
			app.updateDatabase(history);
			app.getConnection().commit();
		}

		// 52番目のレコードを書き換える
		{
			History history = expected.get(52);
			history.recipientPhoneNumber = "TEST_NUMBER";
			history.charge = 55899988;
			history.df = false;
			history.paymentCategorty = "C";
			history.timeSecs = 22551;
			app.updateDatabase(history);
			app.getConnection().commit();
		}

		// アプリによる更新後の値が期待した値であることの確認
		List<History> actual = getHistories();
		assertEquals(expected.size(), actual.size());
		for(int i = 0; i < expected.size(); i++) {
			assertEquals(expected.get(i), actual.get(i), " i = " + i);
		}
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
		ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
		Updater updater = new HistoryUpdateApp(config, new Random(), accessor).new Updater1();

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
		ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
		Updater updater = new HistoryUpdateApp(config, random, accessor).new Updater2();

		History history = toHistory("00000000391", "00000000105", "R", "2020-11-02 06:25:57.430", 1688, null, false);
		History expected = history.clone();

		// 通話時間が変わっていることを確認
		random.setValues(960);
		updater.update(history);
		expected.timeSecs = 961;
		assertEquals(expected, history);

		// 通話時間が変わっていることを確認
		random.setValues(3148);
		updater.update(history);
		expected.timeSecs = 3149;
		assertEquals(expected, history);
	}
}
