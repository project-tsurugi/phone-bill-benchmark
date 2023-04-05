package com.tsurugidb.benchmark.phonebill.db.dao;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTarget;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.entity.History;

/**
 * デバッグ用のHistoryDao
 */
public class HistoryDaoDebug implements HistoryDao {
	private HistoryDao dao;
	private Map<CalculationTarget, Set<History>> resultGetHistoriesTarget = new HashMap<>();

	public HistoryDaoDebug(HistoryDao dao) {
		this.dao = dao;
	}


	@Override
	public int[] batchInsert(Collection<History> histories) {
		return dao.batchInsert(histories);
	}

	@Override
	public int insert(History history) {
		return dao.insert(history);
	}

	@Override
	public long getMaxStartTime() {
		return dao.getMaxStartTime();
	}

	@Override
	public int update(History history) {
		return dao.update(history);
	}

	@Override
	public int batchUpdate(List<History> histories) {
		return dao.batchUpdate(histories);
	}

	@Override
	public List<History> getHistories(Key key) {
		return dao.getHistories(key);
	}

	@Override
	public List<History> getHistories(CalculationTarget target) {
		List<History> result = dao.getHistories(target);
		Set<History> actual = new HashSet<>(result);
		Set<History> expect = resultGetHistoriesTarget.get(target);
		if (expect == null) {
			resultGetHistoriesTarget.put(target, actual);
		} else if (!expect.equals(actual)) {
			throw new AssertionError();
		}
		return dao.getHistories(target);
	}

	@Override
	public List<History> getHistories() {
		return dao.getHistories();
	}


	@Override
	public int updateChargeNull() {
		return dao.updateChargeNull();
	}


	@Override
	public int delete(String phoneNumber) {
		return dao.delete(phoneNumber);
	}

	@Override
	public List<String> getAllPhoneNumbers() {
		return dao.getAllPhoneNumbers();
	}

	@Override
	public long count() {
		return dao.count();
	}

	@Override
	public int delete() {
		return dao.delete();
	}
}
