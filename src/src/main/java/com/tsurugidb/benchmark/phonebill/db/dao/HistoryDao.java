package com.tsurugidb.benchmark.phonebill.db.dao;

import java.util.List;

import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTarget;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;

public interface HistoryDao {
	int[] batchInsert(List<History> histories);

	int insert(History history);

	long getMaxStartTime();

	int update(History history);

	int[] batchUpdate(List<History> histories);

	List<History> getHistories(Key key);

	List<History> getHistories(CalculationTarget target);
}
