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

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.AbstractJdbcTestCase;
import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.entity.History.Key;

class CreateTestDataTest extends AbstractJdbcTestCase {

	/**
	 * 設定ファイルで指定したレコード数と同じレコード数のテストデータが作成されること
	 * @throws Exception
	 */
	@Test
	final void test() throws Exception {
		CreateTestData createTestData = new CreateTestData();
		Config config = Config.getConfig();

		createTestData.execute(config);
		assertEquals(config.numberOfContractsRecords, getContracts().size());
		assertEquals(config.numberOfHistoryRecords, getHistories().size());

		config.numberOfHistoryRecords *=2;
		config.numberOfContractsRecords *=2;

		createTestData.execute(config);
		assertEquals(config.numberOfContractsRecords, getContracts().size());
		assertEquals(config.numberOfHistoryRecords, getHistories().size());
	}


	/**
	 * マルチスレッドでテストデータを生成しても
	 * @throws Exception
	 */
	@Test
	final void testMT() throws Exception {
		CreateTestData createTestData = new CreateTestData();
		Config config = Config.getConfig();
		config.createTestDataThreadCount = 2;
		config.maxNumberOfLinesHistoryCsv = 100;

		// 期待値を取得
		createTestData.execute(config);
		Set<Contract> expectedContracts = getContractSet();
		Set<History> expectedHistories = getHistorySet();

		Map<Key, History> expectMap = expectedHistories.stream().collect(Collectors.toMap(h -> h.getKey(), Function.identity()));


		// マルチスレッドで何回テストデータを生成しても毎回同じデータが生成される
		createTestData.execute(config);

		Map<Key, History> actualMap = getHistories().stream()
				.collect(Collectors.toMap(h -> h.getKey(), Function.identity()));

		for(Entry<Key, History> entry: expectMap.entrySet()) {
			History expect = entry.getValue();
			History actual = actualMap.get(entry.getKey());
			assertNotNull(expect);
			System.out.println(expect);
			assertNotNull(actual, "key = " + entry);
			assertEquals(expect, actual);
		}


		assertEquals(expectedContracts, getContractSet());
		assertEquals(expectedHistories, getHistorySet());
	}

}
