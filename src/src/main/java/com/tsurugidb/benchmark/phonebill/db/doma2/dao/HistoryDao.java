package com.tsurugidb.benchmark.phonebill.db.doma2.dao;

import java.util.List;

import com.tsurugidb.benchmark.phonebill.db.doma2.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.doma2.entity.History;

public interface HistoryDao {
	int[] batchInsert(List<History> histories);

	int insert(History history);

	long getMaxStartTime();

	int update(History history);

	List<History> getHistories(Key key);

}
