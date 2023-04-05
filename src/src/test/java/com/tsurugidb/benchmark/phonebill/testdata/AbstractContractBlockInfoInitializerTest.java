package com.tsurugidb.benchmark.phonebill.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;

/**
 * AbstractContractBlockInfoInitializerのサブクラスをまとめてテストする
 *
 */
class AbstractContractBlockInfoInitializerTest {

	private static Config config;


	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		config = Config.getConfig();
	}

	@Test
	final void test() throws Exception {
		// numberOfContractsRecords == blockSize * 10の前後のケースケース
		config.numberOfContractsRecords = ContractInfoReader.getContractBlockSize(config) * 10;
		testSub(config, 10, false);

		config.numberOfContractsRecords = ContractInfoReader.getContractBlockSize(config) * 10 - 1;
		testSub(config, 10, true);

		config.numberOfContractsRecords = ContractInfoReader.getContractBlockSize(config) * 10 + 1;
		testSub(config, 11, true);

		// numberOfContractsRecords == blockSize * 2の前後のケースケース
		config.numberOfContractsRecords = ContractInfoReader.getContractBlockSize(config) * 2;
		testSub(config, 2, false);

		config.numberOfContractsRecords = ContractInfoReader.getContractBlockSize(config) * 2 - 1;
		testSub(config, 2, true);

		config.numberOfContractsRecords = ContractInfoReader.getContractBlockSize(config) * 2 + 1;
		testSub(config, 3, true);

		// numberOfContractsRecords == blockSize の前後のケースケース
		// numberOfContractsRecords < blockSizeはテストデータ生成時のチェックによりテストデータの生成ができない
		// ため実際に発生し得ないケースと考えテスト対象外
		config.numberOfContractsRecords = ContractInfoReader.getContractBlockSize(config);
		testSub(config, 1, false);

		config.numberOfContractsRecords = ContractInfoReader.getContractBlockSize(config) + 1;
		testSub(config, 2, true);
	}

	private void testSub(Config config, int expectedBlockSize, boolean incompleteLastBlock) throws Exception {
		// テストデータをDBに生成
		new CreateTable().execute(config);
		ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
		TestDataGenerator generator = new TestDataGenerator(config, new Random(), accessor);
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			generator.generateContractsToDb(manager);
		}

		// テスト対象クラスのインスタンスを作成
		List<AbstractContractBlockInfoInitializer> list;
		list = new ArrayList<AbstractContractBlockInfoInitializer>();
		list.add(new DefaultContractBlockInfoInitializer(config));
		list.add(new DbContractBlockInfoInitializer(config));

		// init()の実行結果を確認する
		for (AbstractContractBlockInfoInitializer initializer : list) {
			initializer.init();
			assertEquals(expectedBlockSize, initializer.numberOfBlocks, initializer.getClass().toString());
			Set<Integer> expectedSet = incompleteLastBlock ? Collections.singleton(expectedBlockSize - 1)
					: Collections.emptySet();
			assertEquals(expectedSet, initializer.waitingBlocks, initializer.getClass().toString());
			assertEquals(incompleteLastBlock ? expectedBlockSize - 1 : expectedBlockSize,
					initializer.activeBlockNumberHolder.getNumberOfActiveBlacks(), initializer.getClass().toString());
			assertEquals(Collections.emptyList(), initializer.activeBlockNumberHolder.getActiveBlocks(),
					initializer.getClass().toString());
			assertEquals(incompleteLastBlock ? expectedBlockSize - 2 : expectedBlockSize - 1,
					initializer.activeBlockNumberHolder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock(),
					initializer.getClass().toString());
		}

	}

}
