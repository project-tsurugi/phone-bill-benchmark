package com.tsurugidb.benchmark.phonebill.testdata;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.tsurugidb.benchmark.phonebill.app.Config;

/**
 * 契約マスタが指定のConfigでテストデータ生成直後の状態であることを前提に契約のブロック情報の初期化を行うクラス
 *
 */
public class DefaultContractBlockInfoInitializer extends AbstractContractBlockInfoInitializer {
	private Config config;

	public DefaultContractBlockInfoInitializer(Config config) {
		this.config = config;
	}


	@Override
	void init() {
		int blockSize = ContractInfoReader.getContractBlockSize(config);
		activeBlockNumberHolder = new ActiveBlockNumberHolder();
		waitingBlocks = new HashSet<Integer>();
		if (config.numberOfContractsRecords % blockSize == 0) {
			numberOfBlocks = config.numberOfContractsRecords/ blockSize;
			List<Integer> activeBlockList = IntStream.range(0, numberOfBlocks).boxed().collect(Collectors.toList());
			activeBlockNumberHolder.setActiveBlocks(activeBlockList);
		} else {
			numberOfBlocks = config.numberOfContractsRecords / blockSize + 1;
			waitingBlocks.add(numberOfBlocks - 1);
			List<Integer> activeBlockList = IntStream.range(0, numberOfBlocks - 1).boxed().collect(Collectors.toList());
			activeBlockNumberHolder.setActiveBlocks(activeBlockList);
		}
	}

}
