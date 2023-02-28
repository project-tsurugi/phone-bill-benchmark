package com.tsurugidb.benchmark.phonebill.app;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config.TransactionOption;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.TxOption.Table;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.testdata.PhoneNumberGenerator;

public class InsertTest extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(InsertTest.class);


	private static final TxOption OCC  =  TxOption.ofOCC(1, TxLabel.TEST);
	private static final TxOption LTX = TxOption.ofLTX(1, TxLabel.TEST, Table.HISTORY);



	public static void main(String[] args) throws Exception {
		Config config = Config.getConfig();
		config.maxNumberOfLinesHistoryCsv=1;
		new Issue220().execute(config);
	}

	@Override
	public void execute(Config config) throws Exception {
		// テーブルの作成
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			Ddl ddl = manager.getDdl();
			manager.execute(TxOption.of(), () -> {
				ddl.dropTables();
				ddl.createHistoryTable();
			});
		}

		PhoneNumberGenerator phoneNumberGenerator = new PhoneNumberGenerator(config);

		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			HistoryDao dao  = manager.getHistoryDao();
			History h = new History();
			h.setRecipientPhoneNumber(phoneNumberGenerator.to11DigtString(0));
			h.setPaymentCategorty("C");
			h.setStartTime(LocalDateTime.now());
			h.setTimeSecs(100);
			h.setCharge(0);
			h.setDf(0);
			List<History> list = new ArrayList<History>();
			for (int i = 0; i < config.numberOfHistoryRecords; i++) {
					h.setCallerPhoneNumber(phoneNumberGenerator.to11DigtString(i));
					list.add(h.clone());
			}
			TxOption txOption = config.transactionOption == TransactionOption.LTX ? LTX : OCC;
			manager.execute(txOption, () -> {
				dao.batchInsert(list);
			});
		}
		LOG.info("{} records inserted.", config.numberOfHistoryRecords);
	}
}
