/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
