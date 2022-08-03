package com.tsurugidb.benchmark.phonebill.db.interfaces;

public interface DdlLExecutor {
	public void dropTables();

	public void createHistoryTable();

	public void createBillingTable();

	public void createContractsTable();

	/**
	 * インデックスを生成する
	 */
	public void createIndexes();

	/**
	 * 統計情報を更新する
	 */
	public void updateStatistics();

	/**
	 * テストデータのロード後の処理.
	 * <br>
	 * プライマリーキー、インデックスを作成し、統計情報を更新する
	 */
	public void afterLoadData();


	/**
	 * テストデータのロード前の処理.
	 * <br>
	 * プライマリーキー、インデックスとテーブルデータを削除する
	 */
	public void prepareLoadData();
}
