package com.tsurugidb.benchmark.phonebill.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.AbstractJdbcTestCase;
import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;

class DbContractBlockInfoInitializerTest extends AbstractJdbcTestCase {
	// 契約マスタのブロックサイズが100、レコード数が500、電話番号が連続になるようにconfigを指定
	private static Config config;
	static {
		 try {
			config = Config.getConfig();
			config.duplicatePhoneNumberRate = 0;
			config.expirationDateRate = 50;
			config.noExpirationDateRate = 50;
			config.numberOfContractsRecords = 500;
			new CreateTable().execute(config);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	private static PhoneNumberGenerator generator = new PhoneNumberGenerator(config);

	private void initDb(PhoneBillDbManager manager) throws Exception {
		// テストデータの投入
		truncateTable("contracts");
		ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
		TestDataGenerator generator = new TestDataGenerator(config, new Random(), accessor);
		generator.generateContractsToDb(manager);
	}


	/**
	 * 指定の電話番号をDBから削除する
	 *
	 * @param n 削除対象の電話番号を示すlong値
	 * @throws SQLException
	 */
	private void delete(long n) throws SQLException {
		String sql = "delete from contracts where phone_number = " + "'" + generator.getPhoneNumber(n) + "'";
		executeSql(sql);
	}

	@Test
	final void test() throws Exception {
		DbContractBlockInfoInitializer initializer = new DbContractBlockInfoInitializer(config);
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			// 完全なブロックのみ存在するケース
			initDb(manager);
			initializer.init();
			assertEquals(5, initializer.numberOfBlocks);
			assertEquals(Collections.emptySet(), initializer.waitingBlocks);
			assertEquals(5, initializer.activeBlockNumberHolder.getNumberOfActiveBlacks());
			assertEquals(Collections.emptyList(), initializer.activeBlockNumberHolder.getActiveBlocks());
			assertEquals(4, initializer.activeBlockNumberHolder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

			// 最初のブロックの最初の電話番号の契約が欠損しているケース
			initDb(manager);
			delete(0);
			initializer.init();
			assertEquals(5, initializer.numberOfBlocks);
			assertEquals(Collections.singleton(0), initializer.waitingBlocks);
			assertEquals(4, initializer.activeBlockNumberHolder.getNumberOfActiveBlacks());
			assertEquals(Arrays.asList(1, 2, 3, 4), initializer.activeBlockNumberHolder.getActiveBlocks());
			assertEquals(-1, initializer.activeBlockNumberHolder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

			// 最初のブロックの中間のの電話番号の契約が欠損しているケース
			initDb(manager);
			delete(50);
			initializer.init();
			assertEquals(5, initializer.numberOfBlocks);
			assertEquals(Collections.singleton(0), initializer.waitingBlocks);
			assertEquals(4, initializer.activeBlockNumberHolder.getNumberOfActiveBlacks());
			assertEquals(Arrays.asList(1, 2, 3, 4), initializer.activeBlockNumberHolder.getActiveBlocks());
			assertEquals(-1, initializer.activeBlockNumberHolder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

			// 最初のブロックの最後の電話番号の契約が欠損しているケース
			initDb(manager);
			delete(99);
			initializer.init();
			assertEquals(5, initializer.numberOfBlocks);
			assertEquals(Collections.singleton(0), initializer.waitingBlocks);
			assertEquals(4, initializer.activeBlockNumberHolder.getNumberOfActiveBlacks());
			assertEquals(Arrays.asList(1, 2, 3, 4), initializer.activeBlockNumberHolder.getActiveBlocks());
			assertEquals(-1, initializer.activeBlockNumberHolder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

			// ２番目のブロックの最初の電話番号の契約が欠損しているケース
			initDb(manager);
			delete(100);
			initializer.init();
			assertEquals(5, initializer.numberOfBlocks);
			assertEquals(Collections.singleton(1), initializer.waitingBlocks);
			assertEquals(4, initializer.activeBlockNumberHolder.getNumberOfActiveBlacks());
			assertEquals(Arrays.asList(2, 3, 4), initializer.activeBlockNumberHolder.getActiveBlocks());
			assertEquals(0, initializer.activeBlockNumberHolder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

			// ２番目のブロックの中間のの電話番号の契約が欠損しているケース
			initDb(manager);
			delete(150);
			initializer.init();
			assertEquals(5, initializer.numberOfBlocks);
			assertEquals(Collections.singleton(1), initializer.waitingBlocks);
			assertEquals(4, initializer.activeBlockNumberHolder.getNumberOfActiveBlacks());
			assertEquals(Arrays.asList(2, 3, 4), initializer.activeBlockNumberHolder.getActiveBlocks());
			assertEquals(0, initializer.activeBlockNumberHolder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

			// ２番目のブロックの最後の電話番号の契約が欠損しているケース
			initDb(manager);
			delete(199);
			initializer.init();
			assertEquals(5, initializer.numberOfBlocks);
			assertEquals(Collections.singleton(1), initializer.waitingBlocks);
			assertEquals(4, initializer.activeBlockNumberHolder.getNumberOfActiveBlacks());
			assertEquals(Arrays.asList(2, 3, 4), initializer.activeBlockNumberHolder.getActiveBlocks());
			assertEquals(0, initializer.activeBlockNumberHolder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

			// 最後のブロックの最初の電話番号の契約が欠損しているケース
			initDb(manager);
			delete(400);
			initializer.init();
			assertEquals(5, initializer.numberOfBlocks);
			assertEquals(Collections.singleton(4), initializer.waitingBlocks);
			assertEquals(4, initializer.activeBlockNumberHolder.getNumberOfActiveBlacks());
			assertEquals(Collections.emptyList(), initializer.activeBlockNumberHolder.getActiveBlocks());
			assertEquals(3, initializer.activeBlockNumberHolder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

			// 最後のブロックの中間のの電話番号の契約が欠損しているケース
			initDb(manager);
			delete(450);
			initializer.init();
			assertEquals(5, initializer.numberOfBlocks);
			assertEquals(Collections.singleton(4), initializer.waitingBlocks);
			assertEquals(4, initializer.activeBlockNumberHolder.getNumberOfActiveBlacks());
			assertEquals(Collections.emptyList(), initializer.activeBlockNumberHolder.getActiveBlocks());
			assertEquals(3, initializer.activeBlockNumberHolder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

			// 最後のブロックの最後の電話番号の契約が欠損しているケース
			initDb(manager);
			delete(499);
			initializer.init();
			assertEquals(5, initializer.numberOfBlocks);
			assertEquals(Collections.singleton(4), initializer.waitingBlocks);
			assertEquals(4, initializer.activeBlockNumberHolder.getNumberOfActiveBlacks());
			assertEquals(Collections.emptyList(), initializer.activeBlockNumberHolder.getActiveBlocks());
			assertEquals(3, initializer.activeBlockNumberHolder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

			// 複数のブロックの電話番号の契約が欠損しているケース
			initDb(manager);
			delete(125);
			delete(332);
			initializer.init();
			assertEquals(5, initializer.numberOfBlocks);
			assertEquals(new HashSet<Integer>(Arrays.asList(1, 3)), initializer.waitingBlocks);
			assertEquals(3, initializer.activeBlockNumberHolder.getNumberOfActiveBlacks());
			assertEquals(Arrays.asList(2, 4), initializer.activeBlockNumberHolder.getActiveBlocks());
			assertEquals(0, initializer.activeBlockNumberHolder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

			// 同一ブロックの複数の電話番号の契約が欠損しているケース
			initDb(manager);
			delete(125);
			delete(178);
			delete(300);
			delete(332);
			initializer.init();
			assertEquals(5, initializer.numberOfBlocks);
			assertEquals(new HashSet<Integer>(Arrays.asList(1, 3)), initializer.waitingBlocks);
			assertEquals(3, initializer.activeBlockNumberHolder.getNumberOfActiveBlacks());
			assertEquals(Arrays.asList(2, 4), initializer.activeBlockNumberHolder.getActiveBlocks());
			assertEquals(0, initializer.activeBlockNumberHolder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		}
	}
}
