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
