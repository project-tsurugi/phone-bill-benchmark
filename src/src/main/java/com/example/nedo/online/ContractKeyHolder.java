package com.example.nedo.online;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.example.nedo.app.Config;
import com.example.nedo.db.DBUtils;

/**
 * 契約マスタのPrimaryKeyをメモリ上に保持するクラス.
 * <p>
 * オンラインアプリケーションが使用する。
 *
 */
public class ContractKeyHolder {
	private List<Key> keyList;

	public ContractKeyHolder(Config config) throws SQLException {
		String sql = "select phone_number, start_date from contracts order by phone_number, start_date";
		keyList = new ArrayList<>();
		try (Connection conn = DBUtils.getConnection(config);
				Statement stmt = conn.createStatement()) {
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String phoneNumber = rs.getString(1);
				Date startDate = rs.getDate(2);
				keyList.add(createKey(phoneNumber, startDate));
			}
		}
	}

	/**
	 * 保持しているKeyの数を返す
	 *
	 * @return
	 */
	public synchronized int size() {
		return keyList.size();
	}

	/**
	 * リストn番目のKeyを返す
	 *
	 * @param n
	 * @return
	 */
	public synchronized Key get(int n) {
		return keyList.get(n);
	}


	/**
	 * リストにKeyを追加する
	 *
	 * @param key
	 */
	public synchronized void add(Key key) {
		keyList.add(key);
	}

	/**
	 * リストn番目のKeyを指定のKeyに入れ替える
	 *
	 * @param key
	 */
	public synchronized void replace(int n, Key key) {
		if (n < 0 || keyList.size() <= n) {
			throw new IllegalArgumentException("Invalide index: " + n);
		}
		keyList.set(n, key);
	}


	/**
	 * 電話番号と契約開始日を指定してKeyを生成する
	 *
	 * @param phoneNumber
	 * @param startDate
	 * @return
	 */
	public static Key createKey(String phoneNumber, Date startDate) {
		Key key = new Key();
		key.phoneNumber = phoneNumber;
		key.startDate = startDate;
		return key;
	}


	public static class Key {
		public String phoneNumber;
		public Date startDate;
	}
}
