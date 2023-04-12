package com.tsurugidb.benchmark.phonebill.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;

class PhoneNumberGeneratorTest {

	/**
	 * getPhoneNumber()のテスト
	 * @throws IOException
	 */
	@Test
	void testGetPhoneNumber() throws IOException {
		Config config = Config.getConfig();
		config.duplicatePhoneNumberRate = 2;
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
		assertEquals("99999999999", generator.getPhoneNumber(99999999999L));


		Exception e;
		e = assertThrows(RuntimeException.class, () -> generator.getPhoneNumber(100000000000L));
		assertEquals("Out of phone number range: 100000000000", e.getMessage());
		e= assertThrows(RuntimeException.class, () -> generator.getPhoneNumber(-1));
		assertEquals("Out of phone number range: -1", e.getMessage());
	}

	/**
	 * to11DigtString()のテスト
	 *
	 * @throws IOException
	 */
	@Test
	void  testTo11DigtString() throws IOException{
		Config config = Config.getConfig();
		PhoneNumberGenerator generator = new PhoneNumberGenerator(config);

		assertEquals("00000000000", generator.to11DigtString(0));
		assertEquals("00000000001", generator.to11DigtString(1));
		assertEquals("00000000010", generator.to11DigtString(10));
		assertEquals("00000000100", generator.to11DigtString(100));
		assertEquals("00000001000", generator.to11DigtString(1000));
		assertEquals("00000010000", generator.to11DigtString(10000));
		assertEquals("00000100000", generator.to11DigtString(100000));
		assertEquals("00001000000", generator.to11DigtString(1000000));
		assertEquals("00010000000", generator.to11DigtString(10000000));
		assertEquals("00100000000", generator.to11DigtString(100000000));
		assertEquals("01000000000", generator.to11DigtString(1000000000));
		assertEquals("10000000000", generator.to11DigtString(10000000000L));


		assertEquals("00000000009", generator.to11DigtString(9));
		assertEquals("00000000099", generator.to11DigtString(99));
		assertEquals("00000000999", generator.to11DigtString(999));
		assertEquals("00000009999", generator.to11DigtString(9999));
		assertEquals("00000099999", generator.to11DigtString(99999));
		assertEquals("00000999999", generator.to11DigtString(999999));
		assertEquals("00009999999", generator.to11DigtString(9999999));
		assertEquals("00099999999", generator.to11DigtString(99999999));
		assertEquals("00999999999", generator.to11DigtString(999999999L));
		assertEquals("09999999999", generator.to11DigtString(9999999999L));
		assertEquals("99999999999", generator.to11DigtString(99999999999L));
	}

}

