package com.example.nedo.testdata;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.app.ExecutableCommand;

/**
 * @author umega
 *
 */
public class CreateTestDataCsv implements ExecutableCommand {
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

		TestDataGenerator generator = new TestDataGenerator(config);

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
