package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import java.sql.Date;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTarget;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeUtils;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;
import com.tsurugidb.iceaxe.result.TgEntityResultMapping;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariableList;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;

public class HistoryDaoIceaxe implements HistoryDao {
	private final IceaxeUtils utils;
	private final ContractDaoIceaxe contractDaoIceaxe;

	private static final TgEntityResultMapping<History> RESULT_MAPPING =
			TgResultMapping.of(History::new)
			.character("caller_phone_number", History::setCallerPhoneNumber)
			.character("recipient_phone_number", History::setRecipientPhoneNumber)
			.character("payment_categorty", History::setPaymentCategorty)
			.int8("start_time", History::setStartTime)
			.int4("time_secs", History::setTimeSecs)
			.int4("charge", History::setCharge)
			.int4("df", History::setDf);

	private static final TgParameterMapping<History> PARAMETER_MAPPING = TgParameterMapping.of(History.class)
			.add("caller_phone_number", TgDataType.CHARACTER, History::getCallerPhoneNumber)
			.add("recipient_phone_number", TgDataType.CHARACTER, History::getRecipientPhoneNumber)
			.add("payment_categorty", TgDataType.CHARACTER, History::getPaymentCategorty)
			.add("start_time", TgDataType.INT8, History::getStartTimeAsLong)
			.add("time_secs", TgDataType.INT4, History::getTimeSecs)
			.add("charge", TgDataType.INT4, History::getCharge).add("df", TgDataType.INT4, History::getDf);


	public HistoryDaoIceaxe(PhoneBillDbManagerIceaxe manager) {
		utils = new IceaxeUtils(manager);
		contractDaoIceaxe = new ContractDaoIceaxe(manager);
	}


	@Override
	public int[] batchInsert(Collection<History> histories) {
		var ps = createInsertPs();
		return utils.executeAndGetCount(ps, histories);
	}

	@Override
	public int insert(History h) {
		var ps = createInsertPs();
		return utils.executeAndGetCount(ps, h);
	}

	private TsurugiPreparedStatementUpdate1<History> createInsertPs() {
		String sql = "insert into history(caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs, charge, df) "
				+ "values(:caller_phone_number, :recipient_phone_number, :payment_categorty, :start_time, :time_secs, :charge, :df)";
		return utils.createPreparedStatement(sql, PARAMETER_MAPPING);
	}

	private TsurugiPreparedStatementUpdate1<History> createUpdatePs() {
		String sql = "update history"
				+ " set recipient_phone_number = :recipient_phone_number, payment_categorty = :payment_categorty, time_secs = :time_secs, charge = :charge, df = :df"
				+ " where caller_phone_number = :caller_phone_number and start_time = :start_time";
		return utils.createPreparedStatement(sql, PARAMETER_MAPPING);
	}

	@Override
	public long getMaxStartTime() {
		var ps = utils.createPreparedQuery("select max(start_time) as max_start_time from history");
		List<TsurugiResultEntity> list = utils.execute(ps);
		return (Long) list.get(0).findInt8("max_start_time").orElse(0L);
	}

	@Override
	public int update(History history) {
		var ps = createUpdatePs();
		return utils.executeAndGetCount(ps, history);
	}

	@Override
	public int batchUpdate(List<History> histories) {
		// TODO アップデートに成功した件数を返すようにする
		// TODO アップデートに失敗した(アップデートの戻り値が0)例外をスローする
		var ps = createUpdatePs();
		int[] rets = utils.executeAndGetCount(ps, histories);
		return rets.length;
	}

	/**
	 * 指定の契約に紐付く通話履歴を取得する
	 *
	 * TODO: 現状のTsurugiではwhere句付きのJoinが正しく動作しないので、Joinを使用せずに2回に話変えてQueryを実行している。
	 * Tsurugiが想定通りに動作するようになったら、オリジナルのコードに戻す。
	 *
	 */
	@Override
	public List<History> getHistories(Key key) {
		String sql1 = "select end_date from contracts where phone_number = :phone_number and start_date = :start_date";
		var variable1 = TgVariableList.of().character("phone_number").int8("start_date");
		var ps1 = utils.createPreparedQuery(sql1, TgParameterMapping.of(variable1));
		var param1 = TgParameterList.of()
				.add("phone_number", key.getPhoneNumber())
				.add("start_date", key.getStartDate().getTime());
		var list = utils.execute(ps1, param1);
		if (list.isEmpty()) {
			return Collections.emptyList();
		}
		long endDate = list.get(0).getInt8("end_date");
		endDate = endDate == Long.MAX_VALUE ? Long.MAX_VALUE : endDate;

		String sql2 = "select caller_phone_number, recipient_phone_number, payment_categorty, start_time,"
				+ " time_secs, charge, df from history where start_time >= :start_date and start_time < :end_date"
				+ " and caller_phone_number = :phone_number";
		var variable2 = TgVariableList.of().character("phone_number").int8("start_date").int8("end_date");
		var ps = utils.createPreparedQuery(sql2, TgParameterMapping.of(variable2), RESULT_MAPPING);
		var param2 = TgParameterList.of()
				.add("phone_number", key.getPhoneNumber())
				.add("start_date", key.getStartDate().getTime())
				.add("end_date", endDate + DateUtils.A_DAY_IN_MILLISECONDS);
		return utils.execute(ps, param2);
	}

	/**
	 * 指定の契約に紐付く通話履歴を取得する
	 *
	 */
	public List<History> getHistoriesOrg(Key key) {
		String sql = "select"
				+ " h.caller_phone_number, h.recipient_phone_number, h.payment_categorty, h.start_time, h.time_secs,"
				+ " h.charge, h.df" + " from history h"
				+ " inner join contracts c on c.phone_number = h.caller_phone_number"
				+ " where c.start_date < h.start_time and" + " (h.start_time < c.end_date + "
				+ DateUtils.A_DAY_IN_MILLISECONDS + " or c.end_date = " + Long.MAX_VALUE + ")"
				+ " and c.phone_number = :phone_number and c.start_date = :start_date" + " order by h.start_time";

		var variable = TgVariableList.of().character("phone_number").int8("start_date");
		var ps = utils.createPreparedQuery(sql, TgParameterMapping.of(variable), RESULT_MAPPING);
		var param = TgParameterList.of()
				.add("phone_number", key.getPhoneNumber())
				.add("start_date", key.getStartDate().getTime());
		return utils.execute(ps, param);
	}



	@Override
	public List<History> getHistories(CalculationTarget target) {
		Contract contract = target.getContract();
		Date start = target.getStart();
		Date end = target.getEnd();

		String sql = "select caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs,"
				+ " charge, df" + " from history "
				+ "where start_time >= :start and start_time < :end"
				+ " and ((caller_phone_number = :caller_phone_number  and payment_categorty = 'C') "
				+ "  or (recipient_phone_number = :recipient_phone_number and payment_categorty = 'R'))"
				+ " and df = 0";
		var variable = TgVariableList.of().int8("start").int8("end").character("caller_phone_number")
				.character("recipient_phone_number");
		var ps = utils.createPreparedQuery(sql, TgParameterMapping.of(variable), RESULT_MAPPING);
		var param = TgParameterList.of()
				.add("start", start.getTime())
				.add("end", end.getTime())
				.add("caller_phone_number", contract.getPhoneNumber())
				.add("recipient_phone_number",contract.getPhoneNumber());
		return  utils.execute(ps, param);
	}

	@Override
	public List<History> getHistories() {
		String sql = "select caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs, charge, df from history";
		var ps = utils.createPreparedQuery(sql, RESULT_MAPPING);
		return utils.execute(ps);
	}
}
