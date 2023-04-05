package com.tsurugidb.benchmark.phonebill.online;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeTestTools;
import com.tsurugidb.benchmark.phonebill.testdata.AbstractContractBlockInfoInitializer;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.CreateTestData;
import com.tsurugidb.benchmark.phonebill.testdata.DefaultContractBlockInfoInitializer;
import com.tsurugidb.benchmark.phonebill.testdata.HistoryKey;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

public class HistoryInsertAppIceaxeTest  {
	private static String ICEAXE_CONFIG = "src/test/config/iceaxe.properties";

	private Config config = null;
	private ContractBlockInfoAccessor accessor = null;
	private  IceaxeTestTools testTools = null;


	@BeforeEach
	void before() throws Exception {
		// テストデータを入れる
		config = Config.getConfig(ICEAXE_CONFIG);
		config.numberOfContractsRecords = 10;
		config.expirationDateRate =3;
		config.noExpirationDateRate = 3;
		config.duplicatePhoneNumberRate = 2;
		config.numberOfHistoryRecords = 30;
		new CreateTable().execute(config);
		new CreateTestData().execute(config);

		testTools = new IceaxeTestTools(config);

		// 契約ブロック情報の初期化
		AbstractContractBlockInfoInitializer initializer = new DefaultContractBlockInfoInitializer(config);
		accessor = new SingleProcessContractBlockManager(initializer);
	}

	@AfterEach
	void after() {
		if (testTools != null) {
			testTools.close();
		}
	}


	@Test
	void test() throws IOException, SQLException {
		testTools.truncateTable("history");

		config.historyInsertRecordsPerTransaction = 33;
		HistoryInsertApp app = (HistoryInsertApp) HistoryInsertApp
				.createHistoryInsertApps(config, new Random(), accessor, 1).get(0);
		long expectedBaseTime = HistoryInsertApp.getBaseTime(config);

		// exec()呼び出し毎にhistoryのレコードが増えることを確認
		int expectedHistorySize = 0;
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			for (int i = 0; i < 10; i++) {
				app.exec(manager);
				expectedHistorySize += config.historyInsertRecordsPerTransaction;
				assertEquals(expectedHistorySize, testTools.countRecords("history"));
			}
		}
		// ブロック情報がキャッシュされていることの確認
		// 呼び出し前の値をチェック
		assertEquals(1, app.getContractInfoReader().getBlockInfos().getNumberOfActiveBlacks());
		accessor.submit(accessor.getNewBlock()); // 新しいブロックをアクティブなブロックとしてsubmit
		// ブロック情報はキャッシュされているので、submitの結果が反映されない
		assertEquals(1, app.getContractInfoReader().getBlockInfos().getNumberOfActiveBlacks());

		// === atScheduleListCreatedのテスト

		assertEquals(expectedHistorySize, app.getKeySet().size());
		assertEquals(expectedBaseTime, app.getBaseTime());

		// 呼び出し後の値をチェック
		app.atScheduleListCreated(null);
		assertEquals(0, app.getKeySet().size());
		assertEquals(expectedBaseTime + AbstractOnlineApp.CREATE_SCHEDULE_INTERVAL_MILLS, app.getBaseTime());
		// submitの結果が反映されている
		assertEquals(2, app.getContractInfoReader().getBlockInfos().getNumberOfActiveBlacks());
	}

	/**
	 * 重複したキーのデータが作成されないことの確認
	 * @throws SQLException
	 * @throws IOException
	 */
	@Test
	void testDuplicateKey() throws SQLException, IOException {
		testTools.truncateTable("history");
		config.historyInsertRecordsPerTransaction = 1;
		HistoryInsertApp app;
		app = (HistoryInsertApp) HistoryInsertApp.createHistoryInsertApps(config, new Random(0), accessor, 1).get(0);
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {

			app.exec(manager);
			List<History> expected = testTools.getHistoryList();

			// 同じシードの乱数生成器を使うと、同じhistoryデータが生成されることを確認
			testTools.truncateTable("history");
			app = (HistoryInsertApp) HistoryInsertApp.createHistoryInsertApps(config, new Random(0), accessor, 1)
					.get(0);
			app.exec(manager);
			assertEquals(expected, testTools.getHistoryList());

			// app.exec()の実行前にkeySet()にキーを登録しておくと違うhistoryデータが生成されることを確認
			testTools.truncateTable("history");
			app = (HistoryInsertApp) HistoryInsertApp.createHistoryInsertApps(config, new Random(0), accessor, 1)
					.get(0);
			HistoryKey key = new HistoryKey();
			key.startTime = expected.get(0).getStartTime().getTime();
			key.callerPhoneNumber = Integer.parseInt(expected.get(0).getCallerPhoneNumber());
			app.getKeySet().add(key);
			app.exec(manager);
			assertNotEquals(expected, testTools.getHistoryList());
		}
	}



	/**
	 * createHistoryInsertApps()のテスト
	 * @throws Exception
	 */
	@Test
	void testCreateHistoryInsertApps() throws Exception {
		// 初期データだけの場合、baseTimeは、config.historyMaxDateの翌日0時になる
		testCreateHistoryInsertAppsSub(config, DateUtils.nextDate(config.historyMaxDate).getTime());

		// historyInsertAppによるテストデータを投入
		AbstractOnlineApp app = HistoryInsertApp.createHistoryInsertApps(config, new Random(), accessor, 1).get(0);
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			app.exec(manager);
		}

		// テストデータ投入後は、baseTimeが通話開始時刻の最大値になる
		long max = testTools.getHistoryList().stream().mapToLong(h -> h.getStartTime().getTime()).max().getAsLong();
		testCreateHistoryInsertAppsSub(config, max);

		// numに0を指定したときのテスト
		assertEquals(Collections.emptyList(),
				HistoryInsertApp.createHistoryInsertApps(config, new Random(), accessor, 0));
	}


	/**
	 * @param config
	 * @param baseTime
	 * @throws SQLException
	 * @throws IOException
	 */
	private void testCreateHistoryInsertAppsSub(Config config, long baseTime) throws SQLException, IOException {
		List<AbstractOnlineApp> list = HistoryInsertApp.createHistoryInsertApps(config, new Random(), accessor, 10);
		assertEquals(10, list.size());

		List<HistoryInsertApp> historyInsertApps = new ArrayList<>();
		for(AbstractOnlineApp app: list) {
			assertTrue(app instanceof HistoryInsertApp);
			historyInsertApps.add((HistoryInsertApp) app);
		}

		long duration = AbstractOnlineApp.CREATE_SCHEDULE_INTERVAL_MILLS / 10;
		for(int i = 0; i < 10; i++) {
			HistoryInsertApp app = historyInsertApps.get(i);
			assertEquals(baseTime + duration * i, app.getBaseTime());
		}
	}


}
