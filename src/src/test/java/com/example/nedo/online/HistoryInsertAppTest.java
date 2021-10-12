package com.example.nedo.online;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import com.example.nedo.AbstractDbTestCase;
import com.example.nedo.app.Config;
import com.example.nedo.app.CreateTable;
import com.example.nedo.testdata.CreateTestData;

class HistoryInsertAppTest extends AbstractDbTestCase {

	@Test
	void test() throws IOException, SQLException {
		truncateTable("history");

		Config config = Config.getConfig();
		ContractHolder contractHolder = new ContractHolder(config);
		config.historyInsertRecordsPerTransaction = 33;
		AbstractOnlineApp app = HistoryInsertApp.createHistoryInsertApps(contractHolder, config, 0, 1).get(0);

		int expected = 0;
		for (int i = 0; i < 10; i++) {
			app.exec();
			app.getConnection().commit();
			expected+= config.historyInsertRecordsPerTransaction;
			assertEquals(expected, countRecords("history"));
		}
	}


	/**
	 * createHistoryInsertApps()のテスト
	 * @throws Exception
	 */
	@Test
	void testCreateHistoryInsertApps() throws Exception {
		Config config = Config.getConfig();
		new CreateTable().execute(config);
		new CreateTestData().execute(config);

		OptionalLong max = getHistories().stream().mapToLong(h -> h.startTime.getTime()).max();

		ContractHolder contractHolder = new ContractHolder(config);


		List<AbstractOnlineApp> list = HistoryInsertApp.createHistoryInsertApps(contractHolder, config, 0, 10);
		assertEquals(10, list.size());

		List<HistoryInsertApp> historyInsertApps = new ArrayList<>();
		for(AbstractOnlineApp app: list) {
			assertTrue(app instanceof HistoryInsertApp);
			historyInsertApps.add((HistoryInsertApp) app);
		}

		int duration = AbstractOnlineApp.CREATE_SCHEDULE_INTERVAL_MILLS / 10;
		for(int i = 0; i < 10; i++) {
			HistoryInsertApp app = historyInsertApps.get(i);
			assertEquals(max.getAsLong() + duration * i, app.baseTime);
		}
	}

}
