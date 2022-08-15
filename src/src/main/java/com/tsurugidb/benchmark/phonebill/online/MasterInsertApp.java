package com.tsurugidb.benchmark.phonebill.online;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TgTmSettingDummy;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.TestDataGenerator;

/**
 * 指定の頻度で、契約マスタにレコードをインサートするアプリケーション
 *
 */
public class MasterInsertApp extends AbstractOnlineApp {
    private static final Logger LOG = LoggerFactory.getLogger(MasterInsertApp.class);
    private PhoneBillDbManager manager;

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
		manager = PhoneBillDbManager.createPhoneBillDbManager(config);
		testDataGenerator = new TestDataGenerator(config, new Random(config.randomSeed), accessor);
	}

	@Override
	protected void createData() {
		contract = testDataGenerator.getNewContract();
	}

	@Override
	protected void updateDatabase() {
		ContractDao dao = manager.getContractDao();
		int ret = manager.execute(TgTmSettingDummy.getInstance(), () -> dao.batchInsert(contract));
		LOG.debug("ONLINE APP: Insert {} record to contracs(phoneNumber = {}, startDate = {}).", ret,
				contract.phoneNumber, contract.startDate);
	}
}
