package com.example.nedo.online;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.example.nedo.AbstractDbTestCase;
import com.example.nedo.app.Config;
import com.example.nedo.app.CreateTable;
import com.example.nedo.db.DBUtils;
import com.example.nedo.testdata.AbstractContractBlockInfoInitializer;
import com.example.nedo.testdata.ContractBlockInfoAccessor;
import com.example.nedo.testdata.CreateTestData;
import com.example.nedo.testdata.DefaultContractBlockInfoInitializer;
import com.example.nedo.testdata.SingleProcessContractBlockManager;

class HistoryInsertAppTest extends AbstractDbTestCase {
	private Config config = null;
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

		// 契約ブロック情報の初期化
		AbstractContractBlockInfoInitializer initializer = new DefaultContractBlockInfoInitializer(config);
		accessor = new SingleProcessContractBlockManager(initializer);
	}


	@Test
	void test() throws IOException, SQLException {
		truncateTable("history");
		config.historyInsertRecordsPerTransaction = 33;
		AbstractOnlineApp app = HistoryInsertApp.createHistoryInsertApps(config, new Random(), accessor, 1).get(0);

		int expected = 0;
		for (int i = 0; i < 10; i++) {
			app.exec();
			app.getConnection().commit();
			expected += config.historyInsertRecordsPerTransaction;
			assertEquals(expected, countRecords("history"));
		}
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
		AbstractOnlineApp app = HistoryInsertApp.createHistoryInsertApps(config, new Random(), accessor, 1).get(0);
		app.exec();
		app.getConnection().commit();

		// テストデータ投入後は、baseTimeが通話開始時刻の最大値になる
		long max = getHistories().stream().mapToLong(h -> h.startTime.getTime()).max().getAsLong();
		testCreateHistoryInsertAppsSub(config, max);

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
