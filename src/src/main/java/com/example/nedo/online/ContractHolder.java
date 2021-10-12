package com.example.nedo.online;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.db.Contract;
import com.example.nedo.db.Contract.Key;
import com.example.nedo.db.DBUtils;
import com.example.nedo.db.Duration;
import com.example.nedo.testdata.ContractReader;
import com.example.nedo.testdata.TestDataGenerator;

/**
 * 契約マスタをメモリ上に保持するクラス.
 * <p>
 * オンラインアプリケーションが使用する。
 *
 */
public class ContractHolder implements ContractReader {
    private static final Logger LOG = LoggerFactory.getLogger(ContractHolder.class);

//	private Map<Key, Contract> map = new HashMap<Key, Contract>();
	private Map<Key, Contract> map = new ConcurrentHashMap<Key, Contract>();
	private TestDataGenerator testDataGenerator;

	public ContractHolder(Config config) throws SQLException {
		testDataGenerator = new TestDataGenerator(config);
		String sql = "select phone_number, start_date, end_date, charge_rule from contracts order by phone_number, start_date";
		try (Connection conn = DBUtils.getConnection(config);
				Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(sql)) {
			while (rs.next()) {
				Contract c = new Contract();
				c.phoneNumber = rs.getString(1);
				c.startDate = rs.getDate(2);
				c.endDate = rs.getDate(3);
				c.rule = rs.getString(4);
				map.put(c.getKey(), c);
			}
		}
		LOG.info("ContractHolder loaded all contracts records({} records).", map.size());
	}


	/**
	 * 保持しているContractの数を返す
	 *
	 * @return
	 */
	public int size() {
		return map.size();
	}

	/**
	 * n番目のContractを返す
	 *
	 * @param n
	 * @return
	 */
	public Contract get(int n) {
		Key key = getKey(n);
		return map.get(key);
	}

	/**
	 * n番目のレコードのキーを返す
	 *
	 * @param n
	 * @return
	 */
	private Key getKey(int n) {
		Key key = new Key();
		key.phoneNumber = testDataGenerator.getPhoneNumberAsLong(n);
		key.startDate = testDataGenerator.getDuration(n).getStatDate();
		return key;
	}


	/**
	 * リストにContractを追加する
	 *
	 * @param c
	 */
	public void add(Contract c) {
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
		return map.size();
	}

	@Override
	public synchronized Duration getDurationByPos(int n) {
		Key key = getKey(n);
		Contract c = map.get(key);
		Duration d = new Duration(c.startDate, c.endDate);
		return d;
	}

}
