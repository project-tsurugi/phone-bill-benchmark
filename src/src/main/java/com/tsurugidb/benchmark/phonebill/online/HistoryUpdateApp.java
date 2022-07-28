package com.tsurugidb.benchmark.phonebill.online;

import java.io.IOException;
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

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.jdbc.History;
import com.tsurugidb.benchmark.phonebill.db.jdbc.Contract.Key;
import com.tsurugidb.benchmark.phonebill.testdata.CallTimeGenerator;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.ContractInfoReader;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class HistoryUpdateApp extends AbstractOnlineApp {
	private static final Logger LOG = LoggerFactory.getLogger(HistoryUpdateApp.class);

	private ContractInfoReader contractInfoReader;
	private CallTimeGenerator callTimeGenerator;
	private Updater[] updaters = { new Updater1(), new Updater2() };
	private History history;
	private Random random;

	public HistoryUpdateApp(Config config, Random random, ContractBlockInfoAccessor accessor)
			throws SQLException, IOException {
		super(config.historyUpdateRecordsPerMin, config, random);
		this.callTimeGenerator = CallTimeGenerator.createCallTimeGenerator(random, config);
		this.contractInfoReader = ContractInfoReader.create(config, accessor, random);
		this.random = random;
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
			ps.setInt(5, history.df ? 1 : 0);
			ps.setString(6, history.callerPhoneNumber);
			ps.setTimestamp(7, history.startTime);
			ps.executeUpdate();
		}
	}

	/**
	 * 発信者電話番号と通話開始時刻が指定の契約に合致するレコードを取得する
	 *
	 * @param key
	 * @return
	 * @throws SQLException
	 */
	List<History> getHistories(Key key) throws SQLException {
		List<History> list = new ArrayList<History>();
		String sql = "select" + " h.recipient_phone_number, h.payment_categorty, h.start_time, h.time_secs,"
				+ " h.charge, h.df" + " from history h"
				+ " inner join contracts c on c.phone_number = h.caller_phone_number"
				+ " where c.start_date < h.start_time and" + " (h.start_time < c.end_date + 1"
				+ " or c.end_date is null)" + " and c.phone_number = ? and c.start_date = ?" + " order by h.start_time";
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
			Key key = contractInfoReader.getKeyUpdatingContract();

			// 通話履歴テーブルから、当該契約の有効期間内に当該契約の電話番号で発信した履歴を取り出す
			if (skipDatabaseAccess) {
				return;
			}
			histories = getHistories(key);
		}

		// 取り出した履歴から1レコードを取り出し更新する
		history = histories.get(random.nextInt(histories.size()));

		Updater updater = updaters[random.nextInt(updaters.length)];
		updater.update(history);
	}

	@Override
	protected void updateDatabase() throws SQLException {
		updateDatabase(history);
		LOG.debug("ONLINE APP: Update 1 record from history(callerPhoneNumber = {}, startTime = {}).",
				history.callerPhoneNumber, history.startTime);
	}

	/**
	 * スケジュール作成時に、契約マスタのブロック情報をアップデートする
	 *
	 * @throws IOException
	 */
	@Override
	protected void atScheduleListCreated(List<Long> scheduleList) throws IOException {
		// TODO UT追加
		contractInfoReader.loadActiveBlockNumberList();
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
