package com.tsurugidb.benchmark.phonebill.online;

import java.io.IOException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption.Table;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
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

	public MasterInsertApp(Config config, Random random, ContractBlockInfoAccessor accessor) throws IOException {
		super(config.masterInsertRecordsPerMin, config, random);
		testDataGenerator = new TestDataGenerator(config, new Random(config.randomSeed), accessor);
	}

	@Override
	protected void createData(ContractDao contractDao, HistoryDao historyDao) {
		contract = testDataGenerator.getNewContract();
	}

	@Override
	protected void updateDatabase(ContractDao contractDao, HistoryDao historyDao) {
		int ret = contractDao.insert(contract);
		LOG.debug("ONLINE APP: Insert {} record to contracts(phoneNumber = {}, startDate = {}).", ret,
				contract.getPhoneNumber(), contract.getStartDate());
	}

	@Override
	public TxLabel getTxLabel() {
		return TxLabel.MASTER_INSERT_APP;
	}

	@Override
	public Table getWritePreserveTable() {
		return Table.CONTRACTS;
	}
}
