package com.tsurugidb.benchmark.phonebill.db.interfaces;

public interface DdlLExecutor {
	/**
	 * 指定のテーブルを削除する。テーブルが存在しない場合もエラーにならない。
	 */
	public void dropTable(String tableName);

	/**
	 * 指定のテーブルをトランケートする。テーブルが存在しない場合もエラーにならない。
	 */
	public void truncateTable(String tableName);

	/**
	 * 指定のテーブルに"_back"をつけたテーブル名のテーブルをcreate table as selectを使用して作成する
	 */
	public void createBackTable(String tableName);

	/**
	 * 指定のテーブルのレコード数をカウントする
	 */
	public int count(String tableName);


	/**
	 * 履歴テーブルの更新されたレコード数をカウントする
	 */
	public int countHistoryUpdated();

	/**
	 * 契約テーブルの更新されたレコード数をカウントする
	 */
	public int countContractsUpdated();

	/**
	 * すべてのテーブルを削除する。テーブルが存在しない場合もエラーにならない。
	 */
	public default void dropTables() {
		dropTable("history");
		dropTable("contracts");
		dropTable("billing");
	}

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
