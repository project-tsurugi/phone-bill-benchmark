package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Date;

import com.tsurugidb.benchmark.phonebill.db.dao.BillingDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;

public class BillingDaoIceaxe implements BillingDao {
	private final PhoneBillDbManagerIceaxe manager;
	private final TsurugiSession session;

	public BillingDaoIceaxe(PhoneBillDbManagerIceaxe manager) {
		this.manager = manager;
		this.session = manager.getSession();
	}

	@Override
	public int insert(Billing billing) {
		String sql = "insert into billing("
				+ "phone_number, "
				+ "target_month, "
				+ "basic_charge, "
				+ "metered_charge, "
				+ "billing_amount, "
				+ "batch_exec_id"
				+ ") values("
				+ ":phone_number, "
				+ ":target_month, "
				+ ":basic_charge, "
				+ ":metered_charge, "
				+ ":billing_amount, "
				+ ":batch_exec_id)";
		TgParameterMapping<Billing> param = TgParameterMapping.of(Billing.class)
				.add("phone_number", TgDataType.CHARACTER, Billing::getPhoneNumber)
				.add("target_month", TgDataType.INT8, Billing::getTargetMonthAsLong)
				.add("basic_charge", TgDataType.INT4, Billing::getBasicCharge)
				.add("metered_charge", TgDataType.INT4, Billing::getMeteredCharge)
				.add("billing_amount", TgDataType.INT4, Billing::getBillingAmount)
				.add("batch_exec_id", TgDataType.CHARACTER, Billing::getBatchExecId);

		try (var ps = session.createPreparedStatement(sql, param)) {
			return ps.executeAndGetCount(manager.getCurrentTransaction(), billing);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}

	}

	@Override
	public int delete(Date targetMonth) {
		String sql = "delete from billing where target_month = :target_month";
		TgParameterMapping<Date> param = TgParameterMapping.of(Date.class)
				.add("target_month", TgDataType.INT8, Date::getTime);

		try (var ps = session.createPreparedStatement(sql, param)) {
			return ps.executeAndGetCount(manager.getCurrentTransaction(), targetMonth);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

}
