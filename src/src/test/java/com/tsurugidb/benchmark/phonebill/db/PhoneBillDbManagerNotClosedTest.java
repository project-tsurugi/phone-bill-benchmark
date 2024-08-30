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
package com.tsurugidb.benchmark.phonebill.db;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;

public class PhoneBillDbManagerNotClosedTest {

	/*
	 * クローズ漏れチェックのテスト
	 */
	@Test
	void testNotClosed() throws IOException {
		PhoneBillDbManager.reportNotClosed();
		assertEquals(0, PhoneBillDbManager.getNotClosed().size());

		// 複数のManagerがクローズされていないケース
		Config config = Config.getConfig();
		PhoneBillDbManager m1 = PhoneBillDbManager.createPhoneBillDbManager(config);
		PhoneBillDbManager m2 = PhoneBillDbManager.createPhoneBillDbManager(config);

		PhoneBillDbManager.reportNotClosed();
		assertEquals(2, PhoneBillDbManager.getNotClosed().size());
		assertTrue(PhoneBillDbManager.getNotClosed().contains(m1));
		assertTrue(PhoneBillDbManager.getNotClosed().contains(m2));

		m1.close();
		PhoneBillDbManager.reportNotClosed();
		assertEquals(1, PhoneBillDbManager.getNotClosed().size());
		assertTrue(PhoneBillDbManager.getNotClosed().contains(m2));

		m2.close();
		PhoneBillDbManager.reportNotClosed();
		assertEquals(0, PhoneBillDbManager.getNotClosed().size());
	}
}
