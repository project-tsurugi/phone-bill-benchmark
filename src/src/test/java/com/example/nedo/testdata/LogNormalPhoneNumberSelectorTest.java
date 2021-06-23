package com.example.nedo.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.junit.jupiter.api.Test;

import com.example.nedo.app.Config;

class LogNormalPhoneNumberSelectorTest {

	@Test
	void testGetContractPos() throws IOException {
		Config config = Config.getConfig();
		config.numberOfContractsRecords = 12;
		TestDataGenerator generator = new TestDataGenerator(config);

		ContractReader contractReader = generator.new ContractReaderImpl();
		List<Integer> contracts = IntStream.range(0, contractReader.getNumberOfContracts()).boxed().collect(Collectors.toList());
		LogNormalDistribution distribution = new LogNormalDistribution(config.callerPhoneNumberScale, config.callerPhoneNumberShape);

		// 想定範囲の結果が返ることを確認
		LogNormalPhoneNumberSelector selector = new LogNormalPhoneNumberSelector(distribution, contractReader, contracts);

		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;

		int loopCount = 1000000;
		int counts[] = new int[config.numberOfContractsRecords];
		for (int i = 0; i < loopCount; i++) {
			int pos = selector.getContractPos();
			min = Math.min(min, pos);
			max = Math.max(max, pos);
			counts[pos]++;
		}
		assertEquals(0, min);
		assertEquals(config.numberOfContractsRecords - 1, max);

		// 分布が想定通りの値であることの確認(誤差10%以内)
		double densities[] = new double[counts.length];
		for (double x = 0; x < counts.length; x = x + 0.00001) {
			densities[(int) x] += distribution.density(x);
		}
		double sum = Arrays.stream(densities).sum();

		for (int x = 0; x < counts.length; x++) {
			double expected = loopCount * densities[x] / sum;
			System.out.println("x = " + x + ", count = " + counts[x] + ", expect = " + expected);
			assertEquals(expected, (double)counts[x], expected / 10d);
		}
	}

}
