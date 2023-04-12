package com.tsurugidb.benchmark.phonebill.db.dao;

public interface Ddl {
	/**
	 * 指定のテーブルを削除する。テーブルが存在しない場合もエラーにならない。
	 */
	public void dropTable(String tableName);

	/**
	 * すべてのテーブルを削除する。テーブルが存在しない場合もエラーにならない。
	 */
	public default void dropTables() {
		dropTable("history");
		dropTable("contracts");
		dropTable("billing");
	}

	/**
	 * 指定の名称のテーブルの有無を調べる
	 *
	 * @param tableName
	 * @return
	 */
	public boolean  tableExists(String tableName);

	/**
	 * 履歴テーブルを作成する
	 */
	public void createHistoryTable();

	/**
	 * 請求テーブルを作成する
	 */
	public void createBillingTable();

	/**
	 * 契約テーブルを作成する
	 */
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
