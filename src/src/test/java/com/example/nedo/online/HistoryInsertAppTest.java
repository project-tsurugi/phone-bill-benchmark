package com.example.nedo.online;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.example.nedo.AbstractDbTestCase;
import com.example.nedo.app.Config;

class HistoryInsertAppTest extends AbstractDbTestCase {

	@Test
	void test() throws IOException, SQLException {
		truncateTable("history");

		Config config = Config.getConfig();
		config.historyInsertRecordsPerTransaction = 33;
		Random random = new Random();
		HistoryInsertApp app = new HistoryInsertApp(config, random);

		int expected = 0;
		for (int i = 0; i < 10; i++) {
			app.exec();
			app.getConnection().commit();
			expected+= config.historyInsertRecordsPerTransaction;
			assertEquals(expected, countRecords("history"));
		}
	}

}
