package com.example.nedo.online;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.db.Contract;
import com.example.nedo.testdata.ContractBlockInfoAccessor;
import com.example.nedo.testdata.TestDataGenerator;

/**
 * 指定の頻度で、契約マスタにレコードをインサートするアプリケーション
 *
 */
public class MasterInsertApp extends AbstractOnlineApp {
    private static final Logger LOG = LoggerFactory.getLogger(MasterInsertApp.class);

    /**
     * 本アプリケーションが使用するTestDataGenerator
     */
    private TestDataGenerator testDataGenerator;


	public MasterInsertApp(Config config, Random random, ContractBlockInfoAccessor accessor) throws SQLException, IOException {
		super(config.masterInsertReccrdsPerMin, config, random);
		testDataGenerator = new TestDataGenerator(config, new Random(config.randomSeed), accessor);
	}

	@Override
	protected void createData() throws SQLException {
		// Notihng to do.
	}

	@Override
	protected void updateDatabase() throws SQLException, IOException {
		Connection conn = getConnection();
		try (PreparedStatement ps = conn.prepareStatement(TestDataGenerator.SQL_INSERT_TO_CONTRACT)) {
			Contract c = testDataGenerator.setContract(ps);
			ps.executeUpdate();
			if (LOG.isDebugEnabled()) {
				LOG.debug("ONLINE APP: Insert 1 record to contracs(phoneNumber = {}, startDate = {}).", c.phoneNumber,
						c.startDate);
			}
		}
	}
}
