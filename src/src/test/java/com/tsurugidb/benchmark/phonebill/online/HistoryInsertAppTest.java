package com.tsurugidb.benchmark.phonebill.online;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.AbstractJdbcTestCase;
import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.jdbc.DBUtils;
import com.tsurugidb.benchmark.phonebill.testdata.AbstractContractBlockInfoInitializer;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.CreateTestData;
import com.tsurugidb.benchmark.phonebill.testdata.DefaultContractBlockInfoInitializer;
import com.tsurugidb.benchmark.phonebill.testdata.HistoryKey;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;

class HistoryInsertAppTest extends AbstractJdbcTestCase {
	private Config config = null;
	private PhoneBillDbManager manager;
	private ContractBlockInfoAccessor accessor = null;

	@BeforeEach
	void before() throws Exception {
		// テストデータを入れる
		config = Config.getConfig();
		config.numberOfContractsRecords = 10;
		config.expirationDateRate =3;
		config.noExpirationDateRate = 3;
		config.duplicatePhoneNumberRate = 2;
		config.numberOfHistoryRecords = 1000;
		manager = PhoneBillDbManager.createPhoneBillDbManager(config);
		new CreateTable().execute(config);
		new CreateTestData().execute(config);

		// 契約ブロック情報の初期化
		AbstractContractBlockInfoInitializer initializer = new DefaultContractBlockInfoInitializer(config);
		accessor = new SingleProcessContractBlockManager(initializer);
	}


	@Test
	void test() throws IOException, SQLException {
		truncateTable("history");
		config.historyInsertRecordsPerTransaction = 33;
		HistoryInsertApp app = (HistoryInsertApp) HistoryInsertApp
				.createHistoryInsertApps(manager, config, new Random(), accessor, 1).get(0);
		long expectedBaseTime = HistoryInsertApp.getBaseTime(manager, config);

		// exec()呼び出し毎にhistoryのレコードが増えることを確認

		int expectedHistorySize = 0;
		for (int i = 0; i < 10; i++) {
			app.exec();
			expectedHistorySize += config.historyInsertRecordsPerTransaction;
			assertEquals(expectedHistorySize, countRecords("history"));
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
		truncateTable("history");
		config.historyInsertRecordsPerTransaction = 1;
		HistoryInsertApp app;
		app = (HistoryInsertApp) HistoryInsertApp.createHistoryInsertApps(manager, config, new Random(0), accessor, 1).get(0);
		app.exec();
		List<History> expected = getHistories();

		// 同じシードの乱数生成器を使うと、同じhistoryデータが生成されることを確認
		truncateTable("history");
		app = (HistoryInsertApp) HistoryInsertApp.createHistoryInsertApps(manager, config, new Random(0), accessor, 1).get(0);
		app.exec();
		assertEquals(expected, getHistories());

		// app.exec()の実行前にkeySet()にキーを登録しておくと違うhistoryデータが生成されることを確認
		truncateTable("history");
		app = (HistoryInsertApp) HistoryInsertApp.createHistoryInsertApps(manager, config, new Random(0), accessor, 1).get(0);
		HistoryKey key = new HistoryKey();
		key.startTime = expected.get(0).startTime.getTime();
		key.callerPhoneNumber = Integer.parseInt(expected.get(0).callerPhoneNumber);
		app.getKeySet().add(key);
		app.exec();
		assertNotEquals(expected, getHistories());
	}



	/**
	 * createHistoryInsertApps()のテスト
	 * @throws Exception
	 */
	@Test
	void testCreateHistoryInsertApps() throws Exception {
		// 初期データだけの場合、baseTimeは、config.historyMaxDateの翌日0時になる
		testCreateHistoryInsertAppsSub(config, DBUtils.nextDate(config.historyMaxDate).getTime());

		// historyInsertAppによるテストデータを投入
		AbstractOnlineApp app = HistoryInsertApp.createHistoryInsertApps(manager, config, new Random(), accessor, 1).get(0);
		app.exec();

		// テストデータ投入後は、baseTimeが通話開始時刻の最大値になる
		long max = getHistories().stream().mapToLong(h -> h.startTime.getTime()).max().getAsLong();
		testCreateHistoryInsertAppsSub(config, max);

		// numに0を指定したときのテスト
		assertEquals(Collections.emptyList(),
				HistoryInsertApp.createHistoryInsertApps(manager, config, new Random(), accessor, 0));
	}


	/**
	 * @param config
	 * @param baseTime
	 * @throws SQLException
	 * @throws IOException
	 */
	private void testCreateHistoryInsertAppsSub(Config config, long baseTime) throws SQLException, IOException {
		List<AbstractOnlineApp> list = HistoryInsertApp.createHistoryInsertApps(manager, config, new Random(), accessor, 10);
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
