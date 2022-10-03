package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import java.sql.Date;

import com.tsurugidb.benchmark.phonebill.db.dao.BillingDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeUtils;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;

public class BillingDaoIceaxe implements BillingDao {
	private final IceaxeUtils utils;

	public BillingDaoIceaxe(PhoneBillDbManagerIceaxe manager) {
		utils = new IceaxeUtils(manager);
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
		var ps = utils.createPreparedStatement(sql, param);
		return utils.executeAndGetCount(ps, billing);
	}

	@Override
	public int delete(Date targetMonth) {
		String sql = "delete from billing where target_month = :target_month";
		TgParameterMapping<Date> param = TgParameterMapping.of(Date.class)
				.add("target_month", TgDataType.INT8, Date::getTime);
		var ps = utils.createPreparedStatement(sql, param);
		return utils.executeAndGetCount(ps, targetMonth);
	}

}
