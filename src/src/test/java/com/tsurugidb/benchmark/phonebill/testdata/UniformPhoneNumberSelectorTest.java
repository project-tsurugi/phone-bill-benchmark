package com.tsurugidb.benchmark.phonebill.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.jdbc.Duration;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;


class UniformPhoneNumberSelectorTest {
	private static final Date DATE01 = DateUtils.toDate("2020-01-01");
	private static final Date DATE02 = DateUtils.toDate("2020-02-02");

	/**
	 * getContractPos()のテスト
	 * @throws IOException
	 */
	@Test
	void testGetContractPos() throws IOException {
		Config config = Config.getConfig();
		PhoneNumberGenerator phoneNumberGenerator = new PhoneNumberGenerator(config);
		ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
		accessor.getNewBlock();
		accessor.submit(0);
		List<Duration> durationList = new ArrayList<>();
		durationList.add(new Duration(DATE01, null));
		durationList.add(new Duration(DATE02, null));
		durationList.add(new Duration(DATE01, DATE02));
		durationList.add(new Duration(DATE01, null));
		durationList.add(new Duration(DATE02, null));
		durationList.add(new Duration(DATE01, DATE02));
		List<Boolean> statusList = new ArrayList<>();
		statusList.add(true);
		statusList.add(true);
		statusList.add(false);
		statusList.add(true);
		statusList.add(true);
		statusList.add(false);

		Random random = new Random();
		ContractInfoReader reader = new ContractInfoReader(durationList, statusList, accessor, phoneNumberGenerator, random);
		UniformPhoneNumberSelector selector = new UniformPhoneNumberSelector(random, reader);

		// getContractPos()が返す値が0～ブロックサイズ-1の間であることを確認する。
		Set<Integer> values = new TreeSet<>();
		for(int i = 0; i < 10000; i++) {
			values.add(selector.getContractPos());
		}
		assertIterableEquals(Arrays.asList(0, 1, 2, 3, 4, 5), values);
	}


}
