package com.example.nedo.multinode.client;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.example.nedo.app.Config;
import com.example.nedo.online.AbstractOnlineApp;

class OnlineAppClientTest {

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@BeforeEach
	void setUp() throws Exception {
	}

	@Test
	final void testCreateStatusMessage() throws SQLException, IOException {

		/**
		 * createStatusMessag()のテストに使うAbstractOnlineAppの実装
		 */
		class TestApp extends AbstractOnlineApp {
			private String baseName;
			private int count;

			public TestApp(String baseName, int count) throws SQLException, IOException {
				super(0, Config.getConfig(), new Random());
				this.baseName = baseName;
				this.count = count;
			}

			@Override
			protected void createData() throws SQLException {
			}

			@Override
			protected void updateDatabase() throws SQLException, IOException {
			}

			@Override
			public int getExecCount() {
				return count;
			}

			@Override
			public String getBaseName() {
				return baseName;
			}
		}

		List<AbstractOnlineApp> list = Arrays.asList(
				new TestApp("Name-C", 21),
				new TestApp("Name-A", 20),
				new TestApp("Name-B", 9),
				new TestApp("Name-A", 5),
				new TestApp("Name-C", 13),
				new TestApp("Name-C", 44)
				);


			Instant now = Instant.now();
			Instant start = now.minus(5518, ChronoUnit.MILLIS);


			String actual = OnlineAppClient.createStatusMessage(start, now, list);
			String expected = "uptime = 5.518 sec, exec count(Name-A = 25, Name-B = 9, Name-C = 78)";
			assertEquals(expected, actual);
	}


}
