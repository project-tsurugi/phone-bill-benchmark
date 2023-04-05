package com.tsurugidb.benchmark.phonebill.app;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.app.Config.DbmsType;
import com.tsurugidb.benchmark.phonebill.app.Config.IsolationLevel;
import com.tsurugidb.benchmark.phonebill.app.Config.TransactionOption;
import com.tsurugidb.benchmark.phonebill.app.Config.TransactionScope;

/**
 * 指定のConfigをもとに、スレッド数、トランザクションオプション、
 * トランザクションが異なるコンフィグレーションファイルを
 * カレントディレクトリに作成する。
 */
public class CreateConfigVariation extends ExecutableCommand {

	public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args[0]);
		CreateConfigVariation createConfigVariation = new CreateConfigVariation();
		createConfigVariation.execute(config);
	}

	@Override
	public void execute(Config config) throws Exception {
		final List<Integer> threadCounts = Arrays.asList(1, 2, 4, 6, 8, 16, 32, 64);
		final List<IsolationLevel> isolationLevels;
		final List<TransactionScope> transactionScopes = Arrays.asList(TransactionScope.WHOLE, TransactionScope.CONTRACT);
		final List<TransactionOption> transactionOptions;
		if (config.dbmsType == DbmsType.POSTGRE_SQL_JDBC || config.dbmsType == DbmsType.POSTGRE_NO_BATCHUPDATE) {
			isolationLevels = Arrays.asList(IsolationLevel.READ_COMMITTED, IsolationLevel.SERIALIZABLE);
			transactionOptions = Collections.singletonList(config.transactionOption);
		} else if (config.dbmsType == DbmsType.ICEAXE) {
			transactionOptions = Arrays.asList(TransactionOption.OCC, TransactionOption.LTX);
			isolationLevels = Collections.singletonList(config.isolationLevel);
		} else {
			throw new RuntimeException("Unsupoorted dbms type: " + config.dbmsType);
		}
		createConfigs(config, isolationLevels, transactionOptions, transactionScopes, threadCounts);
	}

	private void createConfigs(Config config, List<IsolationLevel> isolationLevels,
			List<TransactionOption> transactionOptions, List<TransactionScope> transactionScopes,
			List<Integer> threadCounts) throws IOException {
		for(IsolationLevel isolationLevel: isolationLevels) {
			for (TransactionOption transactionOption : transactionOptions) {
				for (TransactionScope transactionScope : transactionScopes) {
					for(int t: threadCounts) {
						config.isolationLevel = isolationLevel;
						config.transactionOption = transactionOption;
						config.transactionScope = transactionScope;
						config.threadCount = t;
						createConfig(config);
					}
				}
			}
		}
	}

	private void createConfig(Config config) throws IOException {
		final String fmt = "%s-%s-%s-T%02d.properties";
		final String filename = String.format(fmt, config.dbmsType == DbmsType.ICEAXE ? "TG" : "PG",
				config.dbmsType == DbmsType.ICEAXE ? config.transactionOption : config.isolationLevel,
				config.transactionScope, config.threadCount);
		Path path = Path.of(filename);
		Files.writeString(path, config.toString());
	}
}
