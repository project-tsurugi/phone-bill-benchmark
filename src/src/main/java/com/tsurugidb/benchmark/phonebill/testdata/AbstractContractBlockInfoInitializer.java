package com.tsurugidb.benchmark.phonebill.testdata;

import java.sql.SQLException;
import java.util.Set;

/**
 * 契約のブロック情報の初期化を行うクラス
 *
 */
public abstract class AbstractContractBlockInfoInitializer {
	/**
	 * ブロックの総数
	 */
	int numberOfBlocks;

	/**
	 * テストデータ生成中のブロック
	 */
	Set<Integer> waitingBlocks;

	/**
	 * アクティブなブロックの情報
	 */
	ActiveBlockNumberHolder activeBlockNumberHolder;


	/**
	 * 契約のブロック情報を初期化する
	 *
	 * @param config
	 * @throws SQLException
	 */
	abstract void init() throws SQLException;

}
