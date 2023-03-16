package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import java.sql.Date;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeUtils;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
import com.tsurugidb.iceaxe.result.TgEntityResultMapping;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariableList;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;

public class ContractDaoIceaxe implements ContractDao {
	// TODO tsurugiがis not nullを使えないため、nullの代わりにLocalDate.MAXを使用している => is not nullサポート後にnullを使用するように変更する

	private final IceaxeUtils utils;

	public ContractDaoIceaxe(PhoneBillDbManagerIceaxe manager) {
		utils = new IceaxeUtils(manager);
	}

	private static final TgEntityResultMapping<Contract> RESULT_MAPPING =
			TgResultMapping.of(Contract::new)
			.character("phone_number", Contract::setPhoneNumber)
			.date("start_date", Contract::setStartDate)
			.date("end_date", Contract::setEndDate)
			.character("charge_rule", Contract::setRule);

	private static final TgParameterMapping<Contract> PARAMETER_MAPPING = TgParameterMapping.of(Contract.class)
			.add("phone_number", TgDataType.CHARACTER, Contract::getPhoneNumber)
			.add("start_date", TgDataType.DATE, Contract::getStartDateAsLocalDate)
			.add("end_date", TgDataType.DATE, Contract::getEndDateAsLocalDate)
			.add("charge_rule", TgDataType.CHARACTER, Contract::getRule);

	private TsurugiPreparedStatementUpdate1<Contract> createInsertPs() {
		String sql = "insert into contracts(phone_number, start_date, end_date, charge_rule) "
				+ "values(:phone_number, :start_date, :end_date, :charge_rule)";
		return utils.createPreparedStatement(sql, PARAMETER_MAPPING);
	}

	private TsurugiPreparedStatementUpdate1<Contract> createUpdatePs() {
		String sql = "update contracts set end_date = :end_date, charge_rule = :charge_rule"
				+ " where phone_number = :phone_number and start_date = :start_date";
		return utils.createPreparedStatement(sql, PARAMETER_MAPPING);
	}


	@Override
	public int[] batchInsert(Collection<Contract> contracts) {
		return utils.executeAndGetCount(createInsertPs(), contracts);
	}


	@Override
	public int insert(Contract contract) {
		return utils.executeAndGetCount(createInsertPs(), contract);
	}

	@Override
	public int update(Contract contract) {
		return utils.executeAndGetCount(createUpdatePs(), contract);
	}

	@Override
	public List<Contract> getContracts(String phoneNumber) {
		String sql = "select start_date, end_date, charge_rule from contracts where phone_number = :phone_number order by start_date";
		var variable = TgVariableList.of().character("phone_number");
		var ps = utils.createPreparedQuery(sql, TgParameterMapping.of(variable));
		var param = TgParameterList.of().add("phone_number", phoneNumber);
		var list = utils.execute(ps, param);
		return list.stream().map(r -> createContract(phoneNumber, r)).collect(Collectors.toList());
	}

	/**
	 * @param phoneNumber
	 * @param r
	 * @return
	 */
	private Contract createContract(String phoneNumber, TsurugiResultEntity r) {
		Contract c = new Contract();
		c.setPhoneNumber(phoneNumber);
		c.setStartDate(r.getDate("start_date"));
		c.setEndDate(r.getDateOrNull("end_date"));
		c.setRule(r.getCharacterOrNull("charge_rule"));
		return c;
	}

	@Override
	public List<Contract> getContracts(Date start, Date end) {
// TODO: is not nullが使用可能になった後に使用するSQL
//		String sql = "select phone_number, start_date, end_date, charge_rule"
//				+ " from contracts where start_date <= :start_date and ( end_date is null or end_date >= :end_date)"
//				+ " order by phone_number";
		String sql = "select phone_number, start_date, end_date, charge_rule"
				+ " from contracts where start_date <= :start_date and end_date >= :end_date"
				+ " order by phone_number";
		var variable = TgVariableList.of().date("start_date").date("end_date");
		var ps = utils.createPreparedQuery(sql, TgParameterMapping.of(variable), RESULT_MAPPING);
		var param = TgParameterList.of().add("end_date", start.toLocalDate() ).add("start_date", end.toLocalDate());
		return utils.execute(ps, param);
	}

	@Override
	public List<Contract> getContracts() {
		String sql = "select phone_number, start_date, end_date, charge_rule from contracts";
		var ps = utils.createPreparedQuery(sql, RESULT_MAPPING);
		return utils.execute(ps);
	}

	@Override
	public List<String> getAllPhoneNumbers() {
		String sql = "select phone_number from contracts order by phone_number";
		var ps = utils.createPreparedQuery(sql);
		var list = utils.execute(ps);
		return list.stream().map(r -> r.getCharacter("phone_number")).collect(Collectors.toList());
	}
	@Override
	public long count() {
		var ps = utils.createPreparedQuery("select count(*) as cnt from contracts");
		List<TsurugiResultEntity> list = utils.execute(ps);
		return list.get(0).findInt8("cnt").orElse(0L);
	}
}
