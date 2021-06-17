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
		TestDataGenerator generator = new TestDataGenerator(config);
		ContractReader contractReader = generator.new ContractReaderImpl();

		PhoneNumberSelector selector;

		// UniformPhoneNumberSelectorが生成されるケース
		selector = PhoneNumberSelector.createSelector(null, DistributionFunction.UNIFORM, 0, 0, contractReader, 0);
		assertEquals(UniformPhoneNumberSelector.class, selector.getClass());

		// LogNormalPhoneNumberSelectorが選択されるケース
		selector = PhoneNumberSelector.createSelector(new Random(), DistributionFunction.LOGNORMAL, 0, 1,
				contractReader, 0);
		assertEquals(LogNormalPhoneNumberSelector.class, selector.getClass());
	}
}
