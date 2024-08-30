/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
