package com.example.nedo.online;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.db.Contract;
import com.example.nedo.db.Contract.Key;
import com.example.nedo.db.History;
import com.example.nedo.testdata.CallTimeGenerator;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class HistoryUpdateApp extends AbstractOnlineApp {
    private static final Logger LOG = LoggerFactory.getLogger(HistoryUpdateApp.class);

	private ContractHolder contractHolder;
	private Random random;
	private CallTimeGenerator callTimeGenerator;
	private Updater[] updaters = {new Updater1(), new Updater2()};
	private History history;


	public HistoryUpdateApp(ContractHolder contractHolder, Config config, int seed) throws SQLException {
		super(config.historyUpdateRecordsPerMin, config);
		this.random = new Random(seed);
		this.callTimeGenerator = CallTimeGenerator.createCallTimeGenerator(random, config);
		this.contractHolder = contractHolder;
	}



	void updateDatabase(History history) throws SQLException {
		Connection conn = getConnection();
		String sql = "update history"
				+ " set recipient_phone_number = ?, payment_categorty = ?, time_secs = ?, charge = ?, df = ?"
				+ " where caller_phone_number = ? and start_time = ?";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, history.recipientPhoneNumber);
			ps.setString(2, history.paymentCategorty);
			ps.setInt(3, history.timeSecs);
			if (history.charge == null) {
				ps.setNull(4, Types.INTEGER);
			} else {
				ps.setInt(4, history.charge);
			}
			ps.setInt(5, history.df ? 1: 0);
			ps.setString(6, history.callerPhoneNumber);
			ps.setTimestamp(7, history.startTime);
			int ret = ps.executeUpdate();
			if (ret != 1) {
				throw new RuntimeException("Fail to update history: " + history);
			}
		}

	}

	List<History> getHistories(Key key) throws SQLException {
		List<History> list = new ArrayList<History>();
		String sql = "select"
				+ " h.recipient_phone_number, h.payment_categorty, h.start_time, h.time_secs,"
				+ " h.charge, h.df"
				+ " from history h"
				+ " inner join contracts c on c.phone_number = h.caller_phone_number"
				+ " where c.start_date < h.start_time and"
				+ " (h.start_time < c.end_date + 1"
				+ " or c.end_date is null)"
				+ " and c.phone_number = ? and c.start_date = ?";
		Connection conn = getConnection();
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, key.phoneNumber);
			ps.setDate(2, key.startDate);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					History h = new History();
					h.callerPhoneNumber = key.phoneNumber;
					h.recipientPhoneNumber = rs.getString(1);
					h.paymentCategorty = rs.getString(2);
					h.startTime = rs.getTimestamp(3);
					h.timeSecs = rs.getInt(4);
					h.charge = rs.getInt(5);
					if (rs.wasNull()) {
						h.charge = null;
					}
					h.df = rs.getInt(6) == 1;
					list.add(h);
				}
			}
		}
		conn.commit();
		return list;
	}




	@Override
	protected void createData() throws SQLException {
		List<History> histories = Collections.emptyList();
		while (histories.isEmpty()) {
			// 更新対象となる契約を選択
			int n = random.nextInt(contractHolder.size());
			Contract contract = contractHolder.get(n);

			// 通話履歴テーブルから、当該契約の有効期間内に当該契約の電話番号で発信した履歴を取り出す
			histories = getHistories(contract.getKey());
		}

		// 取り出した履歴から1レコードを取り出し更新する
		history = histories.get(random.nextInt(histories.size()));

		Updater updater = updaters[random.nextInt(updaters.length)];
		updater.update(history);
	}

	@Override
	protected void updateDatabase() throws SQLException {
		updateDatabase(history);
		if (LOG.isDebugEnabled()) {
			LOG.debug("ONLINE APP: Update 1 record from history.");
		}
	}


	// 通話履歴を更新するInterfaceと、Interfaceを実装したクラス


	interface Updater {
		/**
		 * 通話履歴を更新する
		 *
		 * @param history
		 */
		void update(History history);
	}

	/**
	 * 削除フラグを立てる
	 *
	 */
	@SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC")
	class Updater1 implements Updater {

		@Override
		public void update(History history) {
			history.df = true;
			history.charge = null;
		}
	}

	/**
	 * 通話時間を変更する
	 */
	class Updater2 implements Updater {
		@Override
		public void update(History history) {
			history.timeSecs = callTimeGenerator.getTimeSecs();
			history.charge = null;
		}
	}
}
