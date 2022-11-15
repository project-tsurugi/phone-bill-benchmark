package com.tsurugidb.benchmark.phonebill.online;

import java.io.IOException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.TestDataGenerator;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

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

	public MasterInsertApp(Config config, Random random, ContractBlockInfoAccessor accessor) throws IOException {
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
		int ret = manager.execute(TxOption.of(Integer.MAX_VALUE, TgTmSetting.ofAlways(TgTxOption.ofOCC())),
				() -> dao.insert(contract));
		LOG.debug("ONLINE APP: Insert {} record to contracs(phoneNumber = {}, startDate = {}).", ret,
				contract.getPhoneNumber(), contract.getStartDate());
	}

	@Override
	protected void atTerminate() {
		manager.close();
	}
}
