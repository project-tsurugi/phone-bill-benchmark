package com.example.nedo.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.example.nedo.app.Config;

class PhoneNumberGeneratorTest {

	/**
	 * getPhoneNumber()のテスト
	 * @throws IOException
	 */
	@Test
	void testGetPhoneNumber() throws IOException {
		Config config = Config.getConfig();
		config.duplicatePhoneNumberRatio = 2;
		config.expirationDateRate = 3;
		config.noExpirationDateRate = 1;

		PhoneNumberGenerator generator = new PhoneNumberGenerator(config);
		assertEquals("00000000000", generator.getPhoneNumber(0));
		assertEquals("00000000001", generator.getPhoneNumber(1));
		assertEquals("00000000002", generator.getPhoneNumber(2));
		assertEquals("00000000003", generator.getPhoneNumber(3));

		assertEquals("00000000005", generator.getPhoneNumber(4));
		assertEquals("00000000005", generator.getPhoneNumber(5));
		assertEquals("00000000007", generator.getPhoneNumber(6));
		assertEquals("00000000007", generator.getPhoneNumber(7));

		assertEquals("00000000008", generator.getPhoneNumber(8));
		assertEquals("00000000009", generator.getPhoneNumber(9));
		assertEquals("00000000010", generator.getPhoneNumber(10));
		assertEquals("00000000011", generator.getPhoneNumber(11));

		assertEquals("00000000013", generator.getPhoneNumber(12));
		assertEquals("00000000013", generator.getPhoneNumber(13));
		assertEquals("00000000015", generator.getPhoneNumber(14));
		assertEquals("00000000015", generator.getPhoneNumber(15));

		assertEquals("00000000016", generator.getPhoneNumber(16));
		assertEquals("00000000017", generator.getPhoneNumber(17));
		assertEquals("00000000018", generator.getPhoneNumber(18));

		Exception e;
		e = assertThrows(RuntimeException.class, () -> generator.getPhoneNumber(100000000000L));
		assertEquals("Out of phone number range: 100000000000", e.getMessage());
		e= assertThrows(RuntimeException.class, () -> generator.getPhoneNumber(-1));
		assertEquals("Out of phone number range: -1", e.getMessage());
	}

}
