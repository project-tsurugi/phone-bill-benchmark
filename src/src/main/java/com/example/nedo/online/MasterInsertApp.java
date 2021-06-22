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
	private ContractKeyHolder contractKeyHolder;

	public MasterInsertApp(ContractKeyHolder contractKeyHolder, Config config, int seed) throws SQLException {
		super(config.masterInsertReccrdsPerMin, config);
		testDataGenerator = new TestDataGenerator(config, seed);
		this.contractKeyHolder = contractKeyHolder;
	}

	@Override
	protected void createData() throws SQLException {
		// Notihng to do.
	}

	@Override
	protected void updateDatabase() throws SQLException {
		Connection conn = getConnection();
		try (PreparedStatement ps = conn.prepareStatement(TestDataGenerator.SQL_INSERT_TO_CONTRACT)) {
			int n = contractKeyHolder.size();
			Contract c = testDataGenerator.setContract(ps, n);
			ps.executeUpdate();
			contractKeyHolder.add(ContractKeyHolder.createKey(c.phoneNumber, c.startDate));
			if (LOG.isDebugEnabled()) {
				LOG.debug("ONLINE APP: Insert 1 record to contracs.");
			}
		}
	}
}
