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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.ExecutableCommand;

public class CreateTestDataCsv extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(CreateTestDataCsv.class);

    public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args);
    	CreateTestDataCsv createTestDataCsv = new CreateTestDataCsv();
		createTestDataCsv.execute(config);
	}


	@Override
	public void execute(Config config) throws Exception {
		Path dir = Paths.get(config.csvDir);
		Files.createDirectories(dir);

		// 古い通話履歴ファイルを削除
		for(Path path: CsvUtils.getHistortyFilePaths(dir)) {
			Files.delete(path);
		}

		int seed = config.randomSeed;
		ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
		TestDataGenerator generator = new TestDataGenerator(config, new Random(seed), accessor);

		// 契約マスタのテストデータ生成
		long startTime = System.currentTimeMillis();
		generator.generateContractsToCsv(CsvUtils.getContractsFilePath(dir));
		LOG.info("Start writing csv  data to directory: " + dir.toAbsolutePath().toString());
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "%,d records generated to contracts.csv in %,.3f sec ";
		LOG.info(String.format(format, config.numberOfContractsRecords, elapsedTime / 1000d));

		// 通話履歴のテストデータを作成
		startTime = System.currentTimeMillis();
		generator.generateHistoryToCsv(dir);
		elapsedTime = System.currentTimeMillis() - startTime;
		format = "%,d records generated to history.csv in %,.3f sec ";
		LOG.info(String.format(format, config.numberOfHistoryRecords, elapsedTime / 1000d));
	}





}
