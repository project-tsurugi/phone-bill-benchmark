package com.tsurugidb.benchmark.phonebill.db.dao;

import java.util.List;

import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTarget;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.entity.History;

public interface HistoryDao {
	int[] batchInsert(List<History> histories);

	int insert(History history);

	long getMaxStartTime();

	int update(History history);

	int batchUpdate(List<History> histories);

	List<History> getHistories(Key key);

	List<History> getHistories(CalculationTarget target);

	// RuntimeExceptionを発生させる => UT専用
	default void throwRuntimeException() {
		throw new RuntimeException();
	}
}