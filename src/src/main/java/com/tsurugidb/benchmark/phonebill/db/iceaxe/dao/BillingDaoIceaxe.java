package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import java.sql.Date;
import java.time.LocalDate;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.db.dao.BillingDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeUtils;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;

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
		TgParameterMapping<Billing> parameterMapping = TgParameterMapping.of(Billing.class)
				.add("phone_number", TgDataType.STRING, Billing::getPhoneNumber)
				.add("target_month", TgDataType.DATE, Billing::getTargetMonthAsLocalDate)
				.add("basic_charge", TgDataType.INT, Billing::getBasicCharge)
				.add("metered_charge", TgDataType.INT, Billing::getMeteredCharge)
				.add("billing_amount", TgDataType.INT, Billing::getBillingAmount)
				.add("batch_exec_id", TgDataType.STRING, Billing::getBatchExecId);
		var ps = utils.createPreparedStatement(sql, parameterMapping);
		return utils.executeAndGetCount(ps, billing);
	}

	@Override
	public int delete(Date targetMonth) {
		String sql = "delete from billing where target_month = :target_month";
		LocalDate localDate = targetMonth.toLocalDate();

		var variables = TgBindVariables.of().addDate("target_month");
		var ps = utils.createPreparedStatement(sql, TgParameterMapping.of(variables));
		var parameter = TgBindParameters.of().add("target_month", localDate);
		return utils.executeAndGetCount(ps, parameter);
	}

	@Override
	public List<Billing> getBillings() {
		var resultMapping = TgResultMapping.of(Billing::new).addString("phone_number", Billing::setPhoneNumber)
				.addDate("target_month", Billing::setTargetMonth).addInt("basic_charge", Billing::setBasicCharge)
				.addInt("metered_charge", Billing::setMeteredCharge).addInt("billing_amount", Billing::setBillingAmount)
				.addString("batch_exec_id", Billing::setBatchExecId);
		String sql = "select phone_number, target_month, basic_charge, metered_charge, billing_amount, batch_exec_id from billing";
		var ps = utils.createPreparedQuery(sql, resultMapping);
		return utils.execute(ps);
	}

	@Override
	public int delete() {
		String sql = "delete from billing";
		var ps = utils.createPreparedStatement(sql);
		return utils.executeAndGetCount(ps);
	}
}
