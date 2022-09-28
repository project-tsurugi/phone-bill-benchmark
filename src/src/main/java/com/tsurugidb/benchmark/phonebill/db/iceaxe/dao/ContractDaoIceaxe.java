package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Date;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
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

public class ContractDaoIceaxe implements ContractDao {
	// TODO tsurugiがis not nullを使えないため、nullの代わりにLong.maxValueを使用している => is not nullサポート後にnullを使用するように変更する

	private final PhoneBillDbManagerIceaxe manager;
	private final TsurugiSession session;

	String sql = "select phone_number, start_date, end_date, charge_rule from contracts";


	private static final TgEntityResultMapping<Contract> RESULT_MAPPING =
			TgResultMapping.of(Contract::new)
			.character("phone_number", Contract::setPhoneNumber)
			.int8("start_date", Contract::setStartDate)
			.int8("end_date", Contract::setEndDate)
			.character("charge_rule", Contract::setRule);

	private static final TgParameterMapping<Contract> PARAMETER_MAPPING = TgParameterMapping.of(Contract.class)
			.add("phone_number", TgDataType.CHARACTER, Contract::getPhoneNumber)
			.add("start_date", TgDataType.INT8, Contract::getStartDateAsLong)
			.add("end_date", TgDataType.INT8, Contract::getEndDateAsLong)
			.add("charge_rule", TgDataType.CHARACTER, Contract::getRule);

	private TsurugiPreparedStatementUpdate1<Contract> createInsertPs() throws IOException {
		String sql = "insert into contracts(phone_number, start_date, end_date, charge_rule) "
				+ "values(:phone_number, :start_date, :end_date, :charge_rule)";
		return session.createPreparedStatement(sql, PARAMETER_MAPPING);
	}

	private TsurugiPreparedStatementUpdate1<Contract> createUpdatePs() throws IOException {
		String sql = "update contracts set end_date = :end_date, charge_rule = :charge_rule"
				+ " where phone_number = :phone_number and start_date = :start_date";
		return session.createPreparedStatement(sql, PARAMETER_MAPPING);
	}


	public ContractDaoIceaxe(PhoneBillDbManagerIceaxe manager) {
			this.manager = manager;
			this.session = manager.getSession();
	}

	@Override
	public int[] batchInsert(Collection<Contract> contracts) {
		int[] ret = new int[contracts.size()];
		try (var ps = createInsertPs() ) {
			int i = 0;
			for (Contract c: contracts)
				ret[i++] = ps.executeAndGetCount(manager.getCurrentTransaction(), c);
			return ret;
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	@Override
	public int insert(Contract contract) {
		try (var ps = createInsertPs() ) {
			return ps.executeAndGetCount(manager.getCurrentTransaction(), contract);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	@Override
	public int update(Contract contract) {
		try (var ps = createUpdatePs() ) {
			return ps.executeAndGetCount(manager.getCurrentTransaction(), contract);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	@Override
	public List<Contract> getContracts(String phoneNumber) {
		String sql = "select start_date, end_date, charge_rule from contracts where phone_number = :phone_number order by start_date";
		var variable = TgVariableList.of().character("phone_number");
        try (var ps = session.createPreparedQuery(sql, TgParameterMapping.of(variable))) {
            var param = TgParameterList.of()
            		.add("phone_number", phoneNumber);
			try (var result = ps.execute(manager.getCurrentTransaction(), param)) {
				return result.getRecordList().stream().map(r -> createContract(phoneNumber, r))
						.collect(Collectors.toList());
			}
        } catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	/**
	 * @param phoneNumber
	 * @param r
	 * @return
	 */
	private Contract createContract(String phoneNumber, TsurugiResultEntity r) {
		Contract c = new Contract();
		c.setPhoneNumber(phoneNumber);
		c.setStartDate(r.getInt8OrNull("start_date"));
		c.setEndDate(r.getInt8OrNull("end_date"));
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
		var variable = TgVariableList.of().int8("start_date").int8("end_date");

        try (var ps = session.createPreparedQuery(sql, TgParameterMapping.of(variable), RESULT_MAPPING)) {
            var param = TgParameterList.of()
            		.add("start_date", start.getTime())
            		.add("end_date", end.getTime());
            try (var result = ps.execute(manager.getCurrentTransaction(), param)) {
            	return result.getRecordList();
            }
        } catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	@Override
	public List<Contract> getContracts() {
		String sql = "select phone_number, start_date, end_date, charge_rule from contracts";
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

	@Override
	public List<String> getAllPhoneNumbers() {
		String sql = "select phone_number from contracts order by phone_number";
        try (var ps = session.createPreparedQuery(sql)) {
            try (var result = ps.execute(manager.getCurrentTransaction())) {
            	return result.getRecordList().stream().map(r -> r.getCharacter("phone_number")).collect(Collectors.toList());
            }
        } catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

}
