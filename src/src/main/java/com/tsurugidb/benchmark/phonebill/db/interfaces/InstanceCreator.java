package com.tsurugidb.benchmark.phonebill.db.interfaces;

import java.util.HashMap;

import com.tsurugidb.benchmark.phonebill.app.Config;

/**
 *  ConfigのDBMSタイプに応じたインスタンスを生成するクラス
 */
public class InstanceCreator<T> {
	HashMap<Config.DbmsType, T> map = new HashMap<>();

	/**
	 * DBMSタイプに応じたインスタンスを登録する
	 *
	 * @param dbmsType
	 * @param object
	 */
	public void addEntry(Config.DbmsType dbmsType, T t) {
		map.put(dbmsType, t);
	}

	/**
	 * 全てのDBMSタイプに対応するエントリが存在することをチェックする
	 */
	public void check() {
		for (Config.DbmsType dbmsType : Config.DbmsType.values()) {
			if (!map.containsKey(dbmsType)) {
				String format = "No class corresponding to DBMS type %s is registered.";
				throw new RuntimeException(String.format(format, dbmsType.name()));
			}
		}
	}

	public T createInstance(Config config) {
		return map.get(config.dbmsType);
	}
}
