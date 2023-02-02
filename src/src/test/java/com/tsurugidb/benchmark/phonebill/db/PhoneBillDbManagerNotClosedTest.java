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
		PhoneBillDbManager.reportNotCloaded();
		assertEquals(0, PhoneBillDbManager.getNotClosed().size());

		// 複数のManagerがクローズされていないケース
		Config config = Config.getConfig();
		PhoneBillDbManager m1 = PhoneBillDbManager.createPhoneBillDbManager(config);
		PhoneBillDbManager m2 = PhoneBillDbManager.createPhoneBillDbManager(config);

		PhoneBillDbManager.reportNotCloaded();
		assertEquals(2, PhoneBillDbManager.getNotClosed().size());
		assertTrue(PhoneBillDbManager.getNotClosed().contains(m1));
		assertTrue(PhoneBillDbManager.getNotClosed().contains(m2));

		m1.close();
		PhoneBillDbManager.reportNotCloaded();
		assertEquals(1, PhoneBillDbManager.getNotClosed().size());
		assertTrue(PhoneBillDbManager.getNotClosed().contains(m2));

		m2.close();
		PhoneBillDbManager.reportNotCloaded();
		assertEquals(0, PhoneBillDbManager.getNotClosed().size());
	}
}
