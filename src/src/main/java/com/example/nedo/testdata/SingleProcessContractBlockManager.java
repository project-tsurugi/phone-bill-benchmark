package com.example.nedo.testdata;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * シングルプロセスで使用するContractBlockInfoAccessor
 * <p>
 * プロセス内で唯一のオブジェクトを共有して使用する。
 *
 */
public class SingleProcessContractBlockManager implements ContractBlockInfoAccessor {
    private static final Logger LOG = LoggerFactory.getLogger(SingleProcessContractBlockManager.class);


	/**
	 * ブロックの総数
	 */
	private int numberOfBlacks;

	/**
	 * テストデータ生成中のブロック
	 */
	private Set<Integer> waitingBlocks;

	/**
	 * アクティブなブロックの情報
	 */
	private ActiveBlockNumberHolder activeBlockNumberHolder;

	/**
	 * 契約マスタが空の状態に合致するインスタンスを生成する
	 *
	 * @param initializer
	 */
	public SingleProcessContractBlockManager() {
		numberOfBlacks = 0;
		waitingBlocks = new HashSet<Integer>();
		activeBlockNumberHolder = new ActiveBlockNumberHolder();
	}


	/**
	 * 指定のInitializeを用いて初期化したインスタンスを生成する
	 *
	 * @param initializer
	 * @throws SQLException
	 */
	public SingleProcessContractBlockManager(AbstractContractBlockInfoInitializer initializer)
			throws SQLException {
		initializer.init();
		numberOfBlacks = initializer.numberOfBlocks;
		waitingBlocks = initializer.waitingBlocks;
		activeBlockNumberHolder = initializer.activeBlockNumberHolder;
	}

	@Override
	public synchronized int getNewBlock() {
		LOG.info("New block released, block no = {}.", numberOfBlacks);
		waitingBlocks.add(numberOfBlacks);
		return numberOfBlacks++;
	}

	@Override
	public synchronized void submit(int blockNumber) {
		LOG.info("A block is submitted, block no = {}.", blockNumber);
		if (!waitingBlocks.contains(blockNumber)) {
			throw new IllegalArgumentException("Not waiting blocks, block number = " + blockNumber);
		}
		waitingBlocks.remove(blockNumber);
		activeBlockNumberHolder.addActiveBlockNumber(blockNumber);
	}

	@Override
	public synchronized ActiveBlockNumberHolder getActiveBlockInfo() {
		return activeBlockNumberHolder.clone();
	}

	/**
	 * @return numberOfBlacks
	 */
	public synchronized int getNumberOfBlacks() {
		return numberOfBlacks;
	}

	/**
	 * @return waitingBlocks
	 */
	public synchronized Set<Integer> getWaitingBlocks() {
		return Collections.unmodifiableSet(new HashSet<Integer>(waitingBlocks));
	}
}
