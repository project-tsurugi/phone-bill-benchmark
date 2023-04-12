package com.tsurugidb.benchmark.phonebill.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.Config.DistributionFunction;

class CallTimeGeneratorTest {


	/**
	 * createCallTimeGenerator()のテスト
	 * @throws IOException
	 */
	@Test
	void testCreateCallTimeGenerator() throws IOException {
		Config config = Config.getConfig();
		CallTimeGenerator callTimeGenerator;

		// UniformCallTimeGeneratorが生成されるケース
		Random random = new Random();
		int maxCallTimeSecs = 100;

		config.callTimeDistribution = DistributionFunction.UNIFORM;
		config.maxCallTimeSecs = maxCallTimeSecs;

		callTimeGenerator= CallTimeGenerator.createCallTimeGenerator(random, config);
		assertEquals(UniformCallTimeGenerator.class, callTimeGenerator.getClass());
		assertEquals(random, ((UniformCallTimeGenerator)callTimeGenerator).random);
		assertEquals(maxCallTimeSecs, ((UniformCallTimeGenerator)callTimeGenerator).maxCallTimeSecs);

		// LogNormalCallTimeGeneratorが生成されるケース
		config.callTimeDistribution = DistributionFunction.LOGNORMAL;
		callTimeGenerator = CallTimeGenerator.createCallTimeGenerator(random, config);
		assertEquals(LogNormalCallTimeGenerator.class, callTimeGenerator.getClass());
	}
}
