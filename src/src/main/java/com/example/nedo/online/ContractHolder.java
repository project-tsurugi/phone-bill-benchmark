package com.example.nedo.online;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.example.nedo.app.Config;
import com.example.nedo.db.Contract;
import com.example.nedo.db.Contract.Key;
import com.example.nedo.db.DBUtils;
import com.example.nedo.db.Duration;
import com.example.nedo.testdata.ContractReader;

/**
 * 契約マスタをメモリ上に保持するクラス.
 * <p>
 * オンラインアプリケーションが使用する。
 *
 */
public class ContractHolder implements ContractReader {
	private List<Contract> list = new ArrayList<Contract>();
	private Map<Key, Contract> map = new HashMap<Contract.Key, Contract>();

	public ContractHolder(Config config) throws SQLException {
		String sql = "select phone_number, start_date, end_date, charge_rule from contracts order by phone_number, start_date";
		list = Collections.synchronizedList(new ArrayList<>());
		try (Connection conn = DBUtils.getConnection(config);
				Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(sql)) {
			while (rs.next()) {
				Contract c = new Contract();
				c.phoneNumber = rs.getString(1);
				c.startDate = rs.getDate(2);
				c.endDate = rs.getDate(3);
				c.rule = rs.getString(4);
				list.add(c);
				map.put(c.getKey(), c);
			}
		}
	}

	/**
	 * 保持しているContractの数を返す
	 *
	 * @return
	 */
	public synchronized int size() {
		return list.size();
	}

	/**
	 * リストn番目のContractを返す
	 *
	 * @param n
	 * @return
	 */
	public synchronized Contract get(int n) {
		return list.get(n);
	}


	/**
	 * リストにContractを追加する
	 *
	 * @param c
	 */
	public synchronized void add(Contract c) {
		list.add(c);
		map.put(c.getKey(), c);
	}

	/**
	 * Contractを指定のContractに入れ替える
	 *
	 * @param Contract
	 */
	public synchronized void replace(Contract c) {
		Contract target = map.get(c.getKey());
		// キー以外の項目をコピーする
		target.endDate = c.endDate;
		target.rule = c.rule;
	}

	@Override
	public synchronized int getNumberOfContracts() {
		return list.size();
	}

	@Override
	public synchronized Duration getDurationByPos(int n) {
		Contract c = list.get(n);
		Duration d = new Duration(c.startDate, c.endDate);
		return d;
	}

	@Override
	public synchronized String getPhoneNumberByPos(int n) {
		return list.get(n).phoneNumber;
	}
}
