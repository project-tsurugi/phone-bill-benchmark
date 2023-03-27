package com.tsurugidb.benchmark.phonebill.db.dao;

import java.util.Collection;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTarget;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.entity.History;

public interface HistoryDao {
	static final String TABLE_NAME = "history";

	int[] batchInsert(Collection<History> histories);

	int insert(History history);

	long getMaxStartTime();

	int update(History history);

	int batchUpdate(List<History> histories);

	List<History> getHistories(Key key);

	List<History> getHistories(CalculationTarget target);

	List<History> getHistories();

	int updateChargeNull();

	int delete(String phoneNumber);

	int delete();

	List<String> getAllPhoneNumbers();

	long count();

	// RuntimeExceptionを発生させる => UT専用
	default void throwRuntimeException() {
		throw new RuntimeException();
	}

}
