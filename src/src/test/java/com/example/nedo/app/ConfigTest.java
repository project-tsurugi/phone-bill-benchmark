package com.example.nedo.app;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.example.nedo.app.Config.TransactionScope;
import com.example.nedo.db.DBUtils;

class ConfigTest {
	private static String NOT_DEFALUT_CONFIG_PATH = "src/test/config/not-default.properties";
	private static String DEFALUT_CONFIG_PATH = "src/test/config/default.properties";
	private static String INCONSISTENT_CONFIG_PATH = "src/test/config/inconsistent.properties";


	/**
	 * 設定ファイルを指定しないケースのテスト
	 * @throws IOException
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 * @throws IllegalAccessException
	 */
	@Test
	void test() throws IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		Config defaultConfig = Config.getConfig();
		checkDefault(defaultConfig);

		String[] args = { NOT_DEFALUT_CONFIG_PATH };
		Config config = Config.getConfig(args);
		checkConfig(config);

		// 引数なしのgetConfig()と空配列を引数に持つgetConfig(String[])は同じ結果を返す
		assertEqualsIgnoreLineSeparator(defaultConfig.toString(), Config.getConfig(new String[0]).toString());

		// テストケースの作成漏れ確認のため、configにデフォルト値以外が指定されていることを確認する
		checkDifferent(defaultConfig, config);
	}

	/**
	 * パラメータの指定に矛盾がある設定ファイルを指定したときのテスト
	 * @throws IOException
	 */
	@Test
	void inconsistentTest() throws IOException {
		String[] args = { INCONSISTENT_CONFIG_PATH };
		Exception e = assertThrows(RuntimeException.class, () -> Config.getConfig(args));
		assertEquals("TransactionScope Contract and sharedConnection cannot be specified at the same time.",
				e.getMessage());
	}


	private void checkDifferent(Config defaultConfig, Config config) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		Map<String, Object> defaultMap = describe(defaultConfig);
		Map<String, Object> map = describe(config);
		assertEquals(defaultMap.keySet(), map.keySet());
		for(String key: defaultMap.keySet()) {
			System.out.println("key = " + key +
					", default = " + defaultMap.get(key) +
					", not default = " + map.get(key));
			assertNotEquals(defaultMap.get(key), map.get(key));
		}
	}

	/**
	 * 指定したconfigのフィールド名とフィールド値のmapを返す.
	 * <br>
	 * 大文字で始まるフィールドは定数フィールドとみなしmapに入れない。
	 *
	 * @param config
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	private Map<String, Object> describe(Config config) throws IllegalArgumentException, IllegalAccessException {
		Map<String, Object> map = new HashMap<>();
		for (Field field : config.getClass().getDeclaredFields()) {
			field.setAccessible(true);
			String key = field.getName();
			Object value = field.get(config);
			if (Character.isLowerCase(key.charAt(0))) {
				map.put(key, value);
			}
		}
		return map;
	}


	/**
	 * 指定のconfigにデフォルト値が設定されていることを確認する
	 *
	 * @param config
	 * @throws IOException
	 */
	private void checkDefault(Config config) throws IOException {
		// 料金計算に関するパラメータ
		assertEquals(DBUtils.toDate("2020-12-01"), config.targetMonth);

		/* 契約マスタ生成に関するパラメータ */
		assertEquals((int) 1e3, config.numberOfContractsRecords);
		assertEquals(10, config.duplicatePhoneNumberRatio);
		assertEquals(30, config.expirationDateRate);
		assertEquals(50, config.noExpirationDateRate);
		assertEquals(DBUtils.toDate("2010-11-11"), config.minDate);
		assertEquals(DBUtils.toDate("2021-03-01"), config.maxDate);

		/* 通話履歴生成に関するパラメータ */
		assertEquals(1000, config.numberOfHistoryRecords);

		/* JDBCに関するパラメータ*/
		assertEquals("jdbc:postgresql://127.0.0.1/phonebill", config.url);
		assertEquals("phonebill", config.user);
		assertEquals("phonebill", config.password);
		assertEquals(Connection.TRANSACTION_READ_COMMITTED, config.isolationLevel);

		/* スレッドに関するパラメータ */
		assertEquals(1, config.threadCount);
		assertEquals(true, config.sharedConnection);


		 /* オンラインアプリケーションに関するパラメータ */
		assertEquals(0, config.masterUpdateRecordsPerMin);
		assertEquals(0, config.masterInsertReccrdsPerMin);
		assertEquals(0, config.historyUpdateRecordsPerMin);
		assertEquals(0, config.historyInsertTransactionPerMin);
		assertEquals(1, config.historyInsertRecordsPerTransaction);

		/* その他のパラメータ */
		assertEquals(0, config.randomSeed);
		assertEquals(TransactionScope.WHOLE, config.transactionScope);

		// toStringのチェック
		Path path = Paths.get(DEFALUT_CONFIG_PATH);
		String expected = new String(Files.readAllBytes(path));
		assertEqualsIgnoreLineSeparator(expected, config.toString());
	}

	/**
	 * 指定のconfigに想定する値が指定されていることを確認する
	 *
	 * @param config
	 * @throws IOException
	 */
	private void checkConfig(Config config) throws IOException {
		// 料金計算に関するパラメータ
		assertEquals(DBUtils.toDate("2030-12-01"), config.targetMonth);

		/* 契約マスタ生成に関するパラメータ */
		assertEquals((int) 1e4, config.numberOfContractsRecords);
		assertEquals(100, config.duplicatePhoneNumberRatio);
		assertEquals(300, config.expirationDateRate);
		assertEquals(500, config.noExpirationDateRate);
		assertEquals(DBUtils.toDate("2020-11-11"), config.minDate);
		assertEquals(DBUtils.toDate("2031-03-01"), config.maxDate);

		/* 通話履歴生成に関するパラメータ */
		assertEquals((int) 1e7, config.numberOfHistoryRecords);

		/* その他のパラメータ */
		assertEquals(1969, config.randomSeed);
		assertEquals(TransactionScope.CONTRACT, config.transactionScope);

		/* スレッドに関するパラメータ */
		assertEquals(10, config.threadCount);
		assertEquals(false, config.sharedConnection);

		/* JDBCに関するパラメータ*/
		assertEquals("jdbc:postgresql://127.0.0.1/mydatabase", config.url);
		assertEquals("myuser", config.user);
		assertEquals("mypassword", config.password);
		assertEquals(Connection.TRANSACTION_SERIALIZABLE, config.isolationLevel);

		// toStringのチェック
		Path path = Paths.get(NOT_DEFALUT_CONFIG_PATH);
		String expected = new String(Files.readAllBytes(path));
		assertEqualsIgnoreLineSeparator(expected, config.toString());
	}

	/**
	 * 改行コード以外の差異がないことを確認する
	 *
	 * @param expected
	 * @param string
	 */
	private void assertEqualsIgnoreLineSeparator(String expected, String actual) {
		String e = expected.replaceAll("[\r\n]+$", System.lineSeparator());
		String a = actual.replaceAll("[\r\n]+$", System.lineSeparator());
		assertEquals(e, a);
	}


	/**
	 * toBoolean()のテスト
	 */
	@Test
	void testToBoolan() {
		assertTrue(Config.toBoolan("true"));
		assertTrue(Config.toBoolan("True"));
		assertTrue(Config.toBoolan("TRUE"));
		assertTrue(Config.toBoolan(" true "));
		assertTrue(Config.toBoolan("yes"));
		assertTrue(Config.toBoolan("1"));

		assertFalse(Config.toBoolan("false"));
		assertFalse(Config.toBoolan("no"));
		assertFalse(Config.toBoolan("0"));

		RuntimeException e = assertThrows(RuntimeException.class, () -> Config.toBoolan("badValue"));
		assertEquals("Illegal property value: badValue", e.getMessage());
	}
}
