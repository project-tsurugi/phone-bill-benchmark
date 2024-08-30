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

class PhoneNumberSelectorTest {

	/**
	 * createSelector()のテスト
	 *
	 * @throws IOException
	 */
	@Test
	void testCreateSelector() throws IOException {
		Config config = Config.getConfig();
		ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
		ContractInfoReader contractInfoReader = ContractInfoReader.create(config, accessor, new Random());

		PhoneNumberSelector selector;

		// UniformPhoneNumberSelectorが生成されるケース
		selector = PhoneNumberSelector.createSelector(null, DistributionFunction.UNIFORM, 0, 0, contractInfoReader);
		assertEquals(UniformPhoneNumberSelector.class, selector.getClass());

		// LogNormalPhoneNumberSelectorが選択されるケース
		selector = PhoneNumberSelector.createSelector(new Random(), DistributionFunction.LOGNORMAL, 0, 1,
				contractInfoReader);
		assertEquals(LogNormalPhoneNumberSelector.class, selector.getClass());
	}
}
