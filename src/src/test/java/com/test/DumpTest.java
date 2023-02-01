package com.test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
import com.tsurugidb.benchmark.phonebill.testdata.CreateTestData;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.Transaction;

public class DumpTest {
	private static final String DUMP_CONFIG = "src/test/config/dump.properties";
	private static final Path OUTDIR = Paths.get("dumpdir");

	@Test
	void test() throws Exception {
		Config config = Config.getConfig(DUMP_CONFIG);
		new CreateTestData().execute(config);
	    doDump(config);
	}



	private void doDump(Config config) {
		PhoneBillDbManagerIceaxe manager = (PhoneBillDbManagerIceaxe) PhoneBillDbManager
				.createPhoneBillDbManager(config);


		manager.execute(TxOption.of(), () -> {
			PreparedStatement prep = null;
			ResultSet results = null;
			try {
				Transaction tx = manager.getCurrentTransaction().getLowTransaction();
				var client = manager.getSession().getLowSqlClient();
				prep = client.prepare("SELECT * FROM history").await();
				results = tx.executeDump(prep, List.of(), OUTDIR).await();

				int count = 0;
				while (results.nextRow()) {
					if (results.nextColumn()) {
						count++;
						var s = results.fetchCharacterValue();
						var dumpFile = Paths.get(s);
						System.out.println("Dumpfile path = " + dumpFile.toAbsolutePath().toString());
						if (count == 3) {
							tx.rollback().await();
							System.exit(1);
						}
					}
				}
				if (count == 0) {
					System.err.println("No data found in the table history.");
				}
			} catch (IOException | ServerException ex) {
				throw new RuntimeException("failed to dump.", ex);
			} catch (InterruptedException ex) {
				throw new RuntimeException("failed to dump.", ex);
			} finally {
				if (prep != null) {
					try {
						prep.close();
					} catch (Exception ignore) {
						ignore.printStackTrace();
					}
				}
				if (results != null) {
					try {
						results.close();
					} catch (Exception ignore) {
						ignore.printStackTrace();
					}
				}
			}
		});
	}

}
