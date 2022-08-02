package com.tsurugidb.benchmark.phonebill.online;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.doma2.entity.Contract;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.TestDataGenerator;

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

    /**
     * 契約情報
     */
    private Contract contract;

	public MasterInsertApp(Config config, Random random, ContractBlockInfoAccessor accessor) throws SQLException, IOException {
		super(config.masterInsertReccrdsPerMin, config, random);
		testDataGenerator = new TestDataGenerator(config, new Random(config.randomSeed), accessor);
	}

	@Override
	protected void createData() throws SQLException, IOException {
		contract = testDataGenerator.getNewContract();
	}

	@Override
	protected void updateDatabase() throws SQLException, IOException {
		Connection conn = getConnection();
		try (PreparedStatement ps = conn.prepareStatement(TestDataGenerator.SQL_INSERT_TO_CONTRACT)) {
			testDataGenerator.setContract(ps, contract);
			ps.executeUpdate();
			LOG.debug("ONLINE APP: Insert 1 record to contracs(phoneNumber = {}, startDate = {}).",
					contract.phoneNumber, contract.startDate);
		}
	}
}
