package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Date;
import java.util.Collection;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTarget;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;
import com.tsurugidb.iceaxe.result.TgEntityResultMapping;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariableList;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;

public class HistoryDaoIceaxe implements HistoryDao {
	private final PhoneBillDbManagerIceaxe manager;
	private final TsurugiSession session;


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
		this.manager = manager;
		this.session = manager.getSession();
	}


	@Override
	public int[] batchInsert(Collection<History> histories) {
		int[] ret = new int[histories.size()];
		try (var ps = createInsertPs() ) {
			int i = 0;
			for (History h: histories)
				ret[i++] = ps.executeAndGetCount(manager.getCurrentTransaction(), h);
			return ret;
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	@Override
	public int insert(History h) {
		try (var ps = createInsertPs() ) {
			return ps.executeAndGetCount(manager.getCurrentTransaction(), h);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	private TsurugiPreparedStatementUpdate1<History> createInsertPs() throws IOException {
		String sql = "insert into history(caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs, charge, df) "
				+ "values(:caller_phone_number, :recipient_phone_number, :payment_categorty, :start_time, :time_secs, :charge, :df)";
		return session.createPreparedStatement(sql, PARAMETER_MAPPING);
	}

	private TsurugiPreparedStatementUpdate1<History> createUpdatePs() throws IOException {
		String sql = "update history"
				+ " set recipient_phone_number = :recipient_phone_number, payment_categorty = :payment_categorty, time_secs = :time_secs, charge = :charge, df = :df"
				+ " where caller_phone_number = :caller_phone_number and start_time = :start_time";
		return session.createPreparedStatement(sql, PARAMETER_MAPPING);
	}



	@Override
	public long getMaxStartTime() {
		try (var ps = session.createPreparedQuery("select max(start_time) as max_start_time from history")) {
			List<TsurugiResultEntity> list = ps.executeAndGetList(manager.getCurrentTransaction());
			if (list.size() == 1) {
				return (Long) list.get(0).findInt8("max_start_time").orElse(0L);
			}
			assert false;
			return 0;
        } catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	@Override
	public int update(History history) {
		try (var ps = createUpdatePs() ) {
			return ps.executeAndGetCount(manager.getCurrentTransaction(), history);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	@Override
	public int batchUpdate(List<History> histories) {
		// TODO アップデートに成功した件数を返すようにする
		// TODO アップデートに失敗した(アップデートの戻り値が0)例外をスローする
		try (var ps = createUpdatePs() ) {
			for (History h: histories) {
				ps.executeAndGetCount(manager.getCurrentTransaction(), h);
			}
			return histories.size();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	/**
	 * 指定の契約に紐付く通話履歴を取得する
	 */
	@Override
	public List<History> getHistories(Key key) {
		String sql = "select"
				+ " h.caller_phone_number, h.recipient_phone_number, h.payment_categorty, h.start_time, h.time_secs,"
				+ " h.charge, h.df" + " from history h"
				+ " inner join contracts c on c.phone_number = h.caller_phone_number"
				+ " where c.start_date < h.start_time and"
				+ " (h.start_time < c.end_date + "+ DateUtils.A_DAY_IN_MILLISECONDS  + " or c.end_date is " + Long.MAX_VALUE + ")"
				+ " and c.phone_number = :phone_number and c.start_date = :start_date" + " order by h.start_time";

		var variable = TgVariableList.of().character("phone_number").int8("start_date");
        try (var ps = session.createPreparedQuery(sql, TgParameterMapping.of(variable), RESULT_MAPPING)) {
            var param = TgParameterList.of()
            		.add("phone_number", key.getPhoneNumber())
            		.add("start_date", key.getStartDate().getTime());
            try (var result = ps.execute(manager.getCurrentTransaction(), param)) {
            	return result.getRecordList();
            } catch (TsurugiTransactionException e) {
    			throw new TsurugiTransactionRuntimeException(e);
			}
        } catch (IOException e) {
			throw new UncheckedIOException(e);
		}
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
		try (var ps = session.createPreparedQuery(sql, TgParameterMapping.of(variable), RESULT_MAPPING)) {
            var param = TgParameterList.of()
            		.add("start", start.getTime())
            		.add("end", end.getTime())
            		.add("caller_phone_number", contract.getPhoneNumber())
            		.add("recipient_phone_number",contract.getPhoneNumber());
            try (var result = ps.execute(manager.getCurrentTransaction(), param)) {
            	return result.getRecordList();
            } catch (TsurugiTransactionException e) {
    			throw new TsurugiTransactionRuntimeException(e);
			}
        } catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public List<History> getHistories() {
		String sql = "select caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs, charge, df from history";
        try (var ps = session.createPreparedQuery(sql, RESULT_MAPPING)) {
            try (var result = ps.execute(manager.getCurrentTransaction())) {
            	return result.getRecordList();
            }
        } catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

}
