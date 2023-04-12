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

import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.jdbc.Duration;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

class LogNormalPhoneNumberSelectorTest {
	private static final Date DATE01 = DateUtils.toDate("2020-01-01");
	private static final Date DATE02 = DateUtils.toDate("2020-02-02");

	@Test
	void testGetContractPos() throws IOException {
		Config config = Config.getConfig();
		config.numberOfContractsRecords = 12;
		config.callerPhoneNumberScale=0;
		config.recipientPhoneNumberScale=0;
		config.callerPhoneNumberShape=1;
		config.recipientPhoneNumberShape=1;

		ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
		accessor.getNewBlock();
		accessor.submit(0);
		PhoneNumberGenerator phoneNumberGenerator = new PhoneNumberGenerator(config);
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
		LogNormalDistribution distribution = new LogNormalDistribution(config.callerPhoneNumberScale, config.callerPhoneNumberShape);
		ContractInfoReader reader = new ContractInfoReader(durationList, statusList, accessor, phoneNumberGenerator, random);
		LogNormalPhoneNumberSelector selector = new LogNormalPhoneNumberSelector(distribution, reader);

		// 想定範囲の結果が返ることを確認
		Set<Integer> values = new TreeSet<>();
		for(int i = 0; i < 10000; i++) {
			values.add(selector.getContractPos());
		}
		assertIterableEquals(Arrays.asList(0, 1, 2, 3, 4, 5), values);


		// 分布が想定通りの値であることの確認(誤差1%以内)
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;

		int loopCount = 1000000;
		int counts[] = new int[reader.getBlockSize()];
		for (int i = 0; i < loopCount; i++) {
			int pos = selector.getContractPos();
			min = Math.min(min, pos);
			max = Math.max(max, pos);
			counts[pos]++;
		}
		assertEquals(0, min);
		assertEquals(reader.getBlockSize() - 1, max);

		double densities[] = new double[counts.length];
		for (double x = 0; x < counts.length; x = x + 0.00001) {
			densities[(int) x] += distribution.density(x);
		}
		double sum = Arrays.stream(densities).sum();

		for (int x = 0; x < counts.length; x++) {
			double expected = loopCount * densities[x] / sum;
			System.out.println("x = " + x + ", count = " + counts[x] + ", expect = " + expected);
			assertEquals(expected, (double)counts[x], expected / 1d);
		}
	}

}
