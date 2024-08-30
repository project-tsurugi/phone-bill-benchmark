/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.benchmark.phonebill.testdata;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.ExecutableCommand;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.testdata.Statistics.Counter;

public class TestDataStatistics extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(TestDataStatistics.class);

    public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args);
		TestDataStatistics testDataStatistics = new TestDataStatistics();
		testDataStatistics.execute(config);
	}

	@Override
	public void execute(Config config) throws Exception {
		int seed = config.randomSeed;
		ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
		TestDataGenerator generator = new TestDataGenerator(config, new Random(seed), accessor);
		generator.setStatisticsOnly(true);

		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			// 契約マスタのテストデータ生成
			generator.generateContractsToDb(manager);

			// 通話履歴のテストデータを作成
			generator.generateHistoryToDb(config);
		}

		// 統計情報を出力
		Statistics statistics = generator.getStatistics();
		String dirString =config.statisticsOutputDir;
		if (dirString != null) {
			Path dir = Paths.get(dirString);
			Files.createDirectories(dir);
			writeStatistics(statistics.getSortedTargetPhoneNumberFrequencies(),
					dir.resolve("phoneNumberFrequencies.csv"));
			writeStatistics(statistics.getSortedCallTimeFrequencies(),
					dir.resolve("callTimeFrequencies.csv"));
		}
		LOG.info(statistics.getReport());
	}

	private <K extends Comparable<? super K>> void writeStatistics(List<Counter<K>> list, Path path)
			throws IOException {
		try (BufferedWriter bw = Files.newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
				StandardOpenOption.TRUNCATE_EXISTING);
				PrintWriter pw = new PrintWriter(bw);) {
			pw.println("key, frequency");
			for (Counter<K> counter : list) {
				pw.println(counter.key + "," + counter.count);
			}
		}
	}
}
