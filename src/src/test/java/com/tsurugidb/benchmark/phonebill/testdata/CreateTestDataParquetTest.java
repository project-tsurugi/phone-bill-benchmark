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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.jdbc.Duration;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

class CreateTestDataParquetTest {
    @TempDir
    Path tempDir;

    @Test
    void createParquetFiles() throws Exception {
        Config config = createSmallUniformConfig();

        new CreateTestDataParquet().execute(config, tempDir);

        assertTrue(Files.size(tempDir.resolve("contracts").resolve("part-00000.parquet")) > 0);
        assertTrue(Files.size(tempDir.resolve("billing").resolve("part-00000.parquet")) > 0);
        assertTrue(Files.list(tempDir.resolve("history")).findAny().isPresent());
        assertEquals(10, countRows(tempDir.resolve("contracts").resolve("part-00000.parquet")));
        assertEquals(10, countRows(tempDir.resolve("billing").resolve("part-00000.parquet")));
        assertEquals(20, countRows(tempDir.resolve("history").resolve("part-00000.parquet")));
    }

    @Test
    void allCsvConfigsCreateParquetFiles() throws Exception {
        Path configDir = findDistConfDir();
        List<Path> configFiles;
        try (Stream<Path> paths = Files.list(configDir)) {
            configFiles = paths
                    .filter(path -> path.getFileName().toString().startsWith("csv_"))
                    .filter(path -> path.getFileName().toString().endsWith(".properties"))
                    .sorted()
                    .toList();
        }
        assertEquals(5, configFiles.size());

        for (Path configFile : configFiles) {
            Config config = Config.getConfig(configFile.toString());
            shrinkForSmoke(config);
            Path outputDir = tempDir.resolve(configFile.getFileName().toString().replace(".properties", ""));

            new CreateTestDataParquet().execute(config, outputDir);

            assertEquals(1_000, countRows(outputDir.resolve("contracts").resolve("part-00000.parquet")),
                    configFile.toString());
            assertEquals(1_000, countRows(outputDir.resolve("billing").resolve("part-00000.parquet")),
                    configFile.toString());
            assertEquals(200, countRows(outputDir.resolve("history").resolve("part-00000.parquet")),
                    configFile.toString());
        }
    }

    @Test
    void onlineOnlyDoesNotCreateBillingParquet() throws Exception {
        Config config = createSmallUniformConfig();
        config.onlineOnly = true;

        new CreateTestDataParquet().execute(config, tempDir);

        assertTrue(Files.size(tempDir.resolve("contracts").resolve("part-00000.parquet")) > 0);
        assertTrue(Files.list(tempDir.resolve("history")).findAny().isPresent());
        assertTrue(Files.notExists(tempDir.resolve("billing")));
    }

    @Test
    void uniformContractInfoReaderDoesNotNeedDurationList() throws Exception {
        Config config = createSmallUniformConfig();
        config.numberOfContractsRecords = 25_000_000;
        config.noExpirationDateRate = 25_000_000;

        ContractInfoReader reader = ContractInfoReader.create(config, new SingleProcessContractBlockManager(),
                new Random(config.randomSeed));

        assertEquals(25_000_000, reader.getBlockSize());
        Duration duration = reader.getInitialDuration(24_999_999);
        assertEquals(config.minDate.getTime(), duration.start);
        assertNotNull(reader);
    }

    @Test
    void createParamsListSkipsHugeUniformDurationValidation() throws Exception {
        Config config = createSmallUniformConfig();
        config.numberOfContractsRecords = 25_000_000;
        config.noExpirationDateRate = 25_000_000;
        config.numberOfHistoryRecords = 100_000_000;

        TestDataGenerator generator = new TestDataGenerator(config, new Random(config.randomSeed),
                new SingleProcessContractBlockManager());

        assertEquals(10_000, generator.createParamsList(10_000).size());
    }

    private static Config createSmallUniformConfig() throws Exception {
        Config config = Config.getConfig();
        config.numberOfContractsRecords = 10;
        config.enableUniformContractDuration = true;
        config.duplicatePhoneNumberRate = 0;
        config.expirationDateRate = 0;
        config.noExpirationDateRate = 10;
        config.minDate = DateUtils.toDate("2010-11-11");
        config.maxDate = DateUtils.toDate("2021-03-01");
        config.numberOfHistoryRecords = 20;
        config.createTestDataThreadCount = 2;
        config.targetMonth = DateUtils.toDate("2020-12-01");
        config.historyMinDate = DateUtils.toDate("2020-12-01");
        config.historyMaxDate = DateUtils.toDate("2020-12-02");
        config.randomSeed = 0;
        return config;
    }

    private static void shrinkForSmoke(Config config) throws Exception {
        config.numberOfContractsRecords = 1_000;
        config.enableUniformContractDuration = true;
        config.duplicatePhoneNumberRate = 0;
        config.expirationDateRate = 0;
        config.noExpirationDateRate = 1_000;
        config.numberOfHistoryRecords = 200;
        config.createTestDataThreadCount = 2;
        config.minDate = DateUtils.toDate("2010-11-11");
        config.maxDate = DateUtils.toDate("2021-03-01");
        config.targetMonth = DateUtils.toDate("2020-12-01");
        config.historyMinDate = DateUtils.toDate("2020-12-01");
        config.historyMaxDate = DateUtils.toDate("2020-12-02");
    }

    private static long countRows(Path file) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(file.toUri()), new Configuration()))) {
            return reader.getFooter().getBlocks().stream().mapToLong(block -> block.getRowCount()).sum();
        }
    }

    private static Path findDistConfDir() {
        Path configDir = Path.of("src/dist/conf");
        if (Files.isDirectory(configDir)) {
            return configDir;
        }
        return Path.of("src/src/dist/conf");
    }
}
