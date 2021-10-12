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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class MasterUpdateApp extends AbstractOnlineApp {
    private static final Logger LOG = LoggerFactory.getLogger(MasterUpdateApp.class);

	private static final long DAY_IN_MILLS = 24 * 3600 * 1000;

	private ContractHolder contractHolder;
	private Config config;
	private Random random;
	private Updater[] updaters = {new Updater1(), new Updater2()};

	public MasterUpdateApp(ContractHolder contractHolder, Config config, int seed) throws SQLException {
		super(config.masterUpdateRecordsPerMin, config);
		this.config = config;
		this.random = new Random(seed);
		this.contractHolder = contractHolder;
	}




	/**
	 * 同一の電話番号の契約のリストから、任意の契約を一つ選択し契約内容を更新する.
	 * <br>
	 * ランダムに選んだ契約をランダムに更新し更新後の値に矛盾がないか調べる。矛盾がある場合は別の値で更新して、
	 * 矛盾がないかを調べる。100回更新しても、矛盾のない値にならなかった場合はnullを返す。
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
			// 契約期間の重複がなければOK
			if (!commonDuration(contract, set)) {
				return contract;
			}
		}
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
	List<Contract> getContracts(Connection conn, String phoneNumber) throws SQLException {
		List<Contract> list = new ArrayList<Contract>();
		String sql = "select start_date, end_date, charge_rule from contracts where phone_number = ?";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, phoneNumber);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					Contract c = new Contract();
					c.phoneNumber = phoneNumber;
					c.startDate = rs.getDate(1);
					c.endDate = rs.getDate(2);
					c.rule = rs.getString(3);
					list.add(c);
				}
			}
		}
		return list;
	}

	@Override
	protected void createData() throws SQLException {
		// Nothing to do
	}

	@Override
	protected void updateDatabase() throws SQLException {

		// 更新対象の電話番号を取得
		int n = random.nextInt(contractHolder.size());
		String phoneNumber = contractHolder.get(n).phoneNumber;

		// 当該電話番号の契約を取得
		Connection conn = getConnection();
		List<Contract> contracts = getContracts(conn, phoneNumber);
		if (contracts.isEmpty()) {
			// 電話番号にマッチする契約がなかったとき(DBに追加される前の電話番号を選んだ場合)
			LOG.warn("No contrct found for phoneNumber = {}.", phoneNumber);
			return;
		}

		// 更新する契約を取得する
		Contract updatingContract = getUpdatingContract(contracts);
		if (updatingContract == null) {
			// 契約の更新に失敗したとき(DBが壊れている可能性が高い)
			LOG.warn("Fail to create valid update contracts for phone number: {}", contracts.get(0).phoneNumber);
			for(Contract c: contracts) {
				LOG.warn("   " + c.toString());
			}
			return;
		}

		// 契約を更新
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
				LOG.debug("ONLINE APP: Update 1 record from contracs(phoneNumber = {}, startDate = {}, endDate = {})."
						, updatingContract.phoneNumber, updatingContract.startDate, updatingContract.endDate);
			}
		}
		contractHolder.replace(updatingContract);
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
	@SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC")
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
