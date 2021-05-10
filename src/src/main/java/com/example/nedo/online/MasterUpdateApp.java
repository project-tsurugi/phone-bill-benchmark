package com.example.nedo.online;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.db.Contract;

public class MasterUpdateApp extends AbstractOnlineApp {
	// TODO 更新した結果、マスタが矛盾した状態になる可能性があるので、矛盾が起きないように修正する。

    private static final Logger LOG = LoggerFactory.getLogger(MasterUpdateApp.class);

	private static final long DAY_IN_MILLS = 24 * 3600 * 1000;

	private ContractKeyHolder contractKeyHolder;
	private Config config;
	private Random random;
	private Updater[] updaters = {new Updater1(), new Updater2()};
	private Contract updatingContract;


	public MasterUpdateApp(ContractKeyHolder contractKeyHolder, Config config, Random random) throws SQLException {
		super(config.masterUpdateRecordsPerMin, config, random);
		this.config = config;
		this.random = random;
		this.contractKeyHolder = contractKeyHolder;
	}




	/**
	 * 同一の電話番号の契約のリストから、任意の契約を一つ選択し契約内容を更新する
	 *
	 * @param contracts 同一の電話番号の契約のリスト
	 * @return 更新した契約
	 * @throws SQLException
	 */
	private Contract getUpdatingContract(List<Contract> contracts) throws SQLException {
		for (int i = 0; i < 100; i++) {
			// 契約を一つ選択して更新する
			Set<Contract> set = new HashSet<Contract>(contracts);
			Contract orgContract = contracts.get(random.nextInt(contracts.size()));
			set.remove(orgContract);
			Contract contract = orgContract.clone();
			Updater updater = updaters[random.nextInt(updaters.length)];
			updater.update(contract);
			// 契約期間の重複がなければDBを更新する
			if (!commonDuration(contract, set)) {
				return contract;
			}
		}
		LOG.warn("Fail to create valid update contracts for phone number: {}", contracts.get(0).phoneNumber);
		return null;
	}


	/**
	 * 指定の契約と、契約のコレクションの契約期間に共通の月がないかを調べる
	 *
	 * @param c
	 * @param contracts
	 * @return 共通の月があるときtrue
	 */
	static boolean commonDuration(Contract contract, Collection<Contract> contracts) {
		long start = toEpochMonth(contract.startDate);
		long end = toEpochMonth(contract.endDate);
		for(Contract target: contracts) {
			long targetStart = toEpochMonth(target.startDate);
			long targetEnd = toEpochMonth(target.endDate);
			if (start <= targetEnd && targetStart <= end) {
				return true;
			}
		}
		return false;
	}

	private static long toEpochMonth(Date date) {
		return date == null ? Long.MAX_VALUE : date.toLocalDate().withDayOfMonth(1).toEpochDay();
	}


	/**
	 * 指定の電話番号の契約を取得する
	 *
	 * @param phoneNumber
	 * @return
	 * @throws SQLException
	 */
	List<Contract> getContracts(String phoneNumber) throws SQLException {
		List<Contract> list = new ArrayList<Contract>();
		Connection conn = getConnection();
		String sql = "select start_date, end_date, charge_rule from contracts where phone_number = ?";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, phoneNumber);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				Contract c = new Contract();
				c.phoneNumber = phoneNumber;
				c.startDate = rs.getDate(1);
				c.endDate = rs.getDate(2);
				c.rule = rs.getString(3);
				list.add(c);
			}
		}
		return list;
	}

	@Override
	protected void createData() throws SQLException {
		// 更新対象の電話番号を取得
		int n = random.nextInt(contractKeyHolder.size());
		String phoneNumber = contractKeyHolder.get(n).phoneNumber;
		List<Contract> contracts = getContracts(phoneNumber);
		// 更新する契約を取得する
		updatingContract = getUpdatingContract(contracts);
	}

	@Override
	protected void updateDatabase() throws SQLException {
		if (updatingContract == null) {
			return;
		}

		Connection conn = getConnection();
		String sql = "update contracts set end_date = ?, charge_rule = ? where phone_number = ? and start_date = ?";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setDate(1, updatingContract.endDate);
			ps.setString(2, updatingContract.rule);
			ps.setString(3, updatingContract.phoneNumber);
			ps.setDate(4, updatingContract.startDate);
			int ret = ps.executeUpdate();
			if (ret != 1) {
				throw new RuntimeException("Fail to update contracts: " + updatingContract);
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("ONLINE APP: Update 1 record from contracs.");
			}
		}
	}

	// 契約を更新するInterfaceと、Interfaceを実装したクラス
	interface Updater {
		/**
		 * Contactの値を更新する
		 *
		 * @param contract
		 */
		void update(Contract contract);
	}

	/**
	 * 契約終了日を削除する
	 *
	 */
	class Updater1 implements Updater {
		@Override
		public void update(Contract contract) {
			contract.endDate = null;
		}
	}

	/**
	 * 契約終了日を設定する
	 *
	 */
	class Updater2 implements Updater {
		@Override
		public void update(Contract contract) {
			long startTime = contract.startDate.getTime();
			int d = (int) ((config.maxDate.getTime() - startTime) / DAY_IN_MILLS);
			int r = random.nextInt(d + 1);
			contract.endDate = new Date(startTime + r * DAY_IN_MILLS);
		}
	}
}
