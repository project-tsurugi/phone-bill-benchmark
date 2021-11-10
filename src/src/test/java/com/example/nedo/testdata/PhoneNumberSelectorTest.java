package com.example.nedo.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.example.nedo.app.Config;
import com.example.nedo.app.Config.DistributionFunction;

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

		// 不正なの分布関数(UNDEFINED)を指定した場合

		Error e = assertThrows(AssertionError.class, () -> PhoneNumberSelector.createSelector(new Random(),
				DistributionFunction.UNDEFINED, 0, 1, contractInfoReader));
		assertEquals("UNDEFINED", e.getMessage());
	}


}
