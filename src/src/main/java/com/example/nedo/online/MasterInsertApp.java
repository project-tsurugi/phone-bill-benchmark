package com.example.nedo.online;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.db.Contract;
import com.example.nedo.testdata.TestDataGenerator;

/**
 * 指定の頻度で、契約マスタにレコードをインサートするアプリケーション
 *
 */
public class MasterInsertApp extends AbstractOnlineApp {
    private static final Logger LOG = LoggerFactory.getLogger(MasterInsertApp.class);

	private TestDataGenerator testDataGenerator;
	private ContractHolder contractHolder;

	public MasterInsertApp(ContractHolder contractHolder, Config config) throws SQLException {
		super(config.masterInsertReccrdsPerMin, config);
		testDataGenerator = new TestDataGenerator(config);
		this.contractHolder = contractHolder;
	}

	@Override
	protected void createData() throws SQLException {
		// Notihng to do.
	}

	@Override
	protected void updateDatabase() throws SQLException {
		Connection conn = getConnection();
		try (PreparedStatement ps = conn.prepareStatement(TestDataGenerator.SQL_INSERT_TO_CONTRACT)) {
			Contract c = null;
			synchronized (contractHolder) {
				int n = contractHolder.size();
				 c = testDataGenerator.setContract(ps, n);
				contractHolder.add(c);
			}
			ps.executeUpdate();
			if (LOG.isDebugEnabled()) {
				LOG.debug("ONLINE APP: Insert 1 record to contracs(phoneNumber = {}, startDate = {}).", c.phoneNumber,
						c.startDate);
			}
		}
	}
}
