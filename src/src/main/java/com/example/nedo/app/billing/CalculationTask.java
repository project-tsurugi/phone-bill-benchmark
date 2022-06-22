package com.example.nedo.app.billing;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.app.Config.TransactionScope;
import com.example.nedo.db.old.Contract;
import com.example.nedo.db.old.DBUtils;

public class CalculationTask implements Callable<Exception> {
    private static final Logger LOG = LoggerFactory.getLogger(CalculationTask.class);
    private Config config;
    private String batchExecId;
    private AtomicBoolean abortRequested;


	/**
	 * 計算対象が格納されているQueue
	 */
	private CalculationTargetQueue queue;


	/**
	 * DBConnection;
	 */
	private Connection conn;


	/**
	 * コンストラクタ
	 *
	 * @param queue
	 * @param conn
	 */
	public CalculationTask(CalculationTargetQueue queue, Connection conn, Config config,
			String batchExecId, AtomicBoolean abortRequested) {
		this.queue = queue;
		this.conn = conn;
		this.config = config;
		this.batchExecId = batchExecId;
		this.abortRequested = abortRequested;
	}


	@Override
	public Exception call() throws Exception {
		try {
			LOG.debug("Calculation task started.");
			for(;;) {
				CalculationTarget target;
				try {
					target = queue.take();
					LOG.debug(queue.getStatus());
				} catch (InterruptedException e) {
					LOG.debug("InterruptedException caught and continue taking calculation_target", e);
					continue;
				}
				if (target.isEndOfTask() || abortRequested.get() == true) {
					LOG.debug("Calculation task finished normally.");
					return null;
				}
				// リトライ回数を指定可能にする
				for(;;) {
					try {
						doCalc(target);
						break;
					} catch (SQLException e) {
						if (DBUtils.isRetriableSQLException(e)
								&& config.transactionScope == TransactionScope.CONTRACT) {
							LOG.debug("Calculation task caught a retriable exception, ErrorCode = {}, SQLStatus = {}.",
									e.getErrorCode(), e.getSQLState(), e);
							conn.rollback();
						} else {
							throw e;
						}
					}
				}
			}
		} catch (Exception e) {
			return e;
		}
	}


	/**
	 * 料金計算のメインロジック
	 *
	 * @param target
	 * @throws SQLException
	 */
	private void doCalc(CalculationTarget target) throws SQLException {
		Contract contract = target.getContract();
		LOG.debug("Start calcuration for  contract: {}.", contract);
		Date start = target.getStart();
		Date end = target.getEnd();
		CallChargeCalculator callChargeCalculator = target.getCallChargeCalculator();
		BillingCalculator billingCalculator = target.getBillingCalculator();

		String sqlSelect = "select caller_phone_number, start_time, time_secs, charge"
				+ " from history "
				+ "where start_time >= ? and start_time < ?"
				+ " and ((caller_phone_number = ? and payment_categorty = 'C') "
				+ "  or (recipient_phone_number = ? and payment_categorty = 'R'))"
				+ " and df = 0";

		String sqlUpdate = "update history set charge = ? where caller_phone_number = ? and start_time = ?";

		try (PreparedStatement psSelect = conn.prepareStatement(sqlSelect);
				PreparedStatement psUpdate = conn.prepareStatement(sqlUpdate)) {
			psSelect.setDate(1, start);
			psSelect.setDate(2, DBUtils.nextDate(end));
			psSelect.setString(3, contract.phoneNumber);
			psSelect.setString(4, contract.phoneNumber);
			try (ResultSet rs = psSelect.executeQuery()) {
				while (rs.next()) {
					int time = rs.getInt("time_secs"); // 通話時間を取得
					if (time < 0) {
						throw new RuntimeException("Negative time: " + time);
					}
					int callCharge = callChargeCalculator.calc(time);
					psUpdate.setInt(1, callCharge);
					psUpdate.setString(2, rs.getString("caller_phone_number"));
					psUpdate.setTimestamp(3, rs.getTimestamp("start_time"));
					psUpdate.addBatch();
					billingCalculator.addCallCharge(callCharge);
				}
			}
			psUpdate.executeBatch();
		}
		updateBilling(conn, contract, billingCalculator, start);
		if (config.transactionScope == TransactionScope.CONTRACT) {
			conn.commit();
		}
		LOG.debug("End calcuration for  contract: {}.", contract);
	}

	/**
	 * Billingテーブルを更新する
	 *
	 * @param conn
	 * @param contract
	 * @param billingCalculator
	 * @param targetMonth
	 * @throws SQLException
	 */
	private void updateBilling(Connection conn, Contract contract, BillingCalculator billingCalculator,
			Date targetMonth) throws SQLException {
		LOG.debug("Inserting to billing table: phone_number = {}, target_month = {}"
				+ ", basic_charge = {}, metered_charge = {}, billing_amount = {}, batch_exec_id = {} ",
				contract.phoneNumber, targetMonth, billingCalculator.getBasicCharge(),
				billingCalculator.getMeteredCharge(), billingCalculator.getBillingAmount(), batchExecId);
		String sql = "insert into billing("
				+ "phone_number, target_month, basic_charge, metered_charge, billing_amount, batch_exec_id)"
				+ " values(?, ?, ?, ?, ?, ?)";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, contract.phoneNumber);
			ps.setDate(2, targetMonth);
			ps.setInt(3, billingCalculator.getBasicCharge());
			ps.setInt(4, billingCalculator.getMeteredCharge());
			ps.setInt(5, billingCalculator.getBillingAmount());
			ps.setString(6, batchExecId);
			ps.executeUpdate();
		}
	}
}
