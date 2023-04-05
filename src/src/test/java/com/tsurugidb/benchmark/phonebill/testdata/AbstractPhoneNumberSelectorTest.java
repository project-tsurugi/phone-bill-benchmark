package com.tsurugidb.benchmark.phonebill.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.jdbc.Duration;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

class AbstractPhoneNumberSelectorTest {
	private static final Date DATE01 = DateUtils.toDate("2020-01-01");
	private static final Date DATE02 = DateUtils.toDate("2020-02-02");
	private static final Date DATE03 = DateUtils.toDate("2020-03-03");
	private static final Date DATE04 = DateUtils.toDate("2020-04-04");
	private static final Date DATE05 = DateUtils.toDate("2020-05-05");
	private static final Date DATE06 = DateUtils.toDate("2020-06-06");


	/**
	 * setup()で初期化されるテスト用のPhoneNumberSelectorのインスタンス、
	 */
	private PhoneNumberSelectorImpl selector;

	/**
	 *
	 */
	private List<Boolean> statusList = new ArrayList<>();

	@BeforeEach
	void setUp() throws Exception {
		Config config = Config.getConfig();
		PhoneNumberGenerator phoneNumberGenerator = new PhoneNumberGenerator(config);
		ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
		accessor.getNewBlock();
		accessor.submit(0);
		List<Duration> durationList = new ArrayList<>();
		durationList.add(new Duration(DATE02, null));
		durationList.add(new Duration(DATE02, DATE04));
		durationList.add(new Duration(DATE03, DATE06));

		statusList.add(true);
		statusList.add(true);
		statusList.add(false);
		Random random = new Random();
		ContractInfoReader reader = new ContractInfoReader(durationList, statusList, accessor, phoneNumberGenerator, random);
		selector = new PhoneNumberSelectorImpl(reader);
	}

	@Test
	void testSelectPhoneNumberLongLong() {
		// exceptPhoneNumberを指定しないケース
		selector.setValues(0, 1, 2);
		assertThrows(RuntimeException.class, () -> selector.selectPhoneNumber(DATE01.getTime(), -1));
		assertThrows(RuntimeException.class, () -> selector.selectPhoneNumber(DATE01.getTime(), -1));
		assertThrows(RuntimeException.class, () -> selector.selectPhoneNumber(DATE01.getTime(), -1));
		selector.setValues(0, 1, 2);
		assertEquals(0, selector.selectPhoneNumber(DATE02.getTime(), -1));
		assertEquals(1, selector.selectPhoneNumber(DATE02.getTime(), -1));
		assertEquals(0, selector.selectPhoneNumber(DATE02.getTime(), -1));
		selector.setValues(0, 1, 2);
		assertEquals(0, selector.selectPhoneNumber(DATE03.getTime(), -1));
		assertEquals(1, selector.selectPhoneNumber(DATE03.getTime(), -1));
		assertEquals(2, selector.selectPhoneNumber(DATE03.getTime(), -1));
		selector.setValues(0, 1, 2);
		assertEquals(0, selector.selectPhoneNumber(DATE04.getTime(), -1));
		assertEquals(2, selector.selectPhoneNumber(DATE04.getTime(), -1));
		assertEquals(2, selector.selectPhoneNumber(DATE04.getTime(), -1));
		selector.setValues(0, 1, 2);
		assertEquals(0, selector.selectPhoneNumber(DATE05.getTime(), -1));
		assertEquals(2, selector.selectPhoneNumber(DATE05.getTime(), -1));
		assertEquals(2, selector.selectPhoneNumber(DATE05.getTime(), -1));
		selector.setValues(0, 1, 2);
		assertEquals(0, selector.selectPhoneNumber(DATE06.getTime(), -1));
		assertEquals(0, selector.selectPhoneNumber(DATE06.getTime(), -1));
		assertEquals(0, selector.selectPhoneNumber(DATE06.getTime(), -1));

		// exceptPhoneNumberを指定するケース
		selector.setValues(0, 1, 2);
		assertThrows(RuntimeException.class, () -> selector.selectPhoneNumber(DATE01.getTime(), 0));
		assertThrows(RuntimeException.class, () -> selector.selectPhoneNumber(DATE01.getTime(), 0));
		assertThrows(RuntimeException.class, () -> selector.selectPhoneNumber(DATE01.getTime(), 0));
		selector.setValues(0, 1, 2, 0, 1, 2);
		assertEquals(1, selector.selectPhoneNumber(DATE02.getTime(), 0));
		assertEquals(1, selector.selectPhoneNumber(DATE02.getTime(), 0));
		assertEquals(1, selector.selectPhoneNumber(DATE02.getTime(), 0));
		assertEquals(0, selector.selectPhoneNumber(DATE02.getTime(), 1));
		assertEquals(0, selector.selectPhoneNumber(DATE02.getTime(), 1));
		assertEquals(0, selector.selectPhoneNumber(DATE02.getTime(), 1));
		selector.setValues(0, 1, 2, 0, 1, 2, 0, 1, 2);
		assertEquals(1, selector.selectPhoneNumber(DATE03.getTime(), 0));
		assertEquals(1, selector.selectPhoneNumber(DATE03.getTime(), 0));
		assertEquals(2, selector.selectPhoneNumber(DATE03.getTime(), 0));
		assertEquals(0, selector.selectPhoneNumber(DATE03.getTime(), 1));
		assertEquals(2, selector.selectPhoneNumber(DATE03.getTime(), 1));
		assertEquals(2, selector.selectPhoneNumber(DATE03.getTime(), 1));
		assertEquals(0, selector.selectPhoneNumber(DATE03.getTime(), 2));
		assertEquals(1, selector.selectPhoneNumber(DATE03.getTime(), 2));
		assertEquals(0, selector.selectPhoneNumber(DATE03.getTime(), 2));
		selector.setValues(0, 1, 2, 0, 1, 2);
		assertEquals(2, selector.selectPhoneNumber(DATE04.getTime(), 0));
		assertEquals(2, selector.selectPhoneNumber(DATE04.getTime(), 0));
		assertEquals(2, selector.selectPhoneNumber(DATE04.getTime(), 01));
		assertEquals(0, selector.selectPhoneNumber(DATE04.getTime(), 2));
		assertEquals(0, selector.selectPhoneNumber(DATE04.getTime(), 2));
		assertEquals(0, selector.selectPhoneNumber(DATE04.getTime(), 2));
		selector.setValues(0, 1, 2, 0, 1, 2);
		assertEquals(2, selector.selectPhoneNumber(DATE05.getTime(), 0));
		assertEquals(2, selector.selectPhoneNumber(DATE05.getTime(), 0));
		assertEquals(2, selector.selectPhoneNumber(DATE05.getTime(), 01));
		assertEquals(0, selector.selectPhoneNumber(DATE05.getTime(), 2));
		assertEquals(0, selector.selectPhoneNumber(DATE05.getTime(), 2));
		assertEquals(0, selector.selectPhoneNumber(DATE05.getTime(), 2));
		selector.setValues(0, 1, 2);
		assertThrows(RuntimeException.class, () -> selector.selectPhoneNumber(DATE06.getTime(), 0));
		assertThrows(RuntimeException.class, () -> selector.selectPhoneNumber(DATE06.getTime(), 0));
		assertThrows(RuntimeException.class, () -> selector.selectPhoneNumber(DATE06.getTime(), 0));
	}

	@Test
	void testSelectPhoneNumberLong() {
		// exceptPhoneNumberを指定しないケース
		selector.setValues(0);
		assertThrows(RuntimeException.class, () -> selector.selectPhoneNumber(DATE01.getTime(), -1));
		selector.setValues(0, 1, 2);
		assertEquals(0, selector.selectPhoneNumber(-1));
		assertEquals(1, selector.selectPhoneNumber(-1));
		assertEquals(0, selector.selectPhoneNumber(-1));

		// exceptPhoneNumberを指定するケース
		selector.setValues(0, 1, 2, 0, 1, 2);
		assertEquals(1, selector.selectPhoneNumber(0));
		assertEquals(1, selector.selectPhoneNumber(0));
		assertEquals(1, selector.selectPhoneNumber(0));
		assertEquals(0, selector.selectPhoneNumber(1));
		assertEquals(0, selector.selectPhoneNumber(1));
		assertEquals(0, selector.selectPhoneNumber(1));

		// activeな契約が1つもない
		statusList.clear();
		statusList.add(false);
		statusList.add(false);
		statusList.add(false);
		selector.setValues(0, 1, 2);
		assertThrows(RuntimeException.class, () -> selector.selectPhoneNumber(-1));
		assertThrows(RuntimeException.class, () -> selector.selectPhoneNumber(-1));
		assertThrows(RuntimeException.class, () -> selector.selectPhoneNumber(-1));

		// activeな契約が1つだけ
		statusList.clear();
		statusList.add(false);
		statusList.add(true);
		statusList.add(false);
		selector.setValues(0, 1, 2);
		assertEquals(1, selector.selectPhoneNumber(-1));
		assertEquals(1, selector.selectPhoneNumber(-1));
		assertEquals(1, selector.selectPhoneNumber(-1));
		assertThrows(RuntimeException.class, () -> selector.selectPhoneNumber(1));
		assertThrows(RuntimeException.class, () -> selector.selectPhoneNumber(1));
		assertThrows(RuntimeException.class, () -> selector.selectPhoneNumber(1));
	}


	/**
	 * テスト用のAbstractPhoneNumberSelectorの実装
	 *
	 * getContractPos()が返す値をsetValues()で指定できる。
	 *
	 */
	private static class PhoneNumberSelectorImpl extends AbstractPhoneNumberSelector {
		Queue<Integer> queue = new LinkedList<Integer>();

		public void setValues(Integer... integers) {
			queue.clear();
			queue.addAll(Arrays.asList(integers));
		}

		public PhoneNumberSelectorImpl(ContractInfoReader contractInfoReader) {
			super(contractInfoReader);
		}

		@Override
		protected int getContractPos() {
			return queue.poll();
		}
	}
}
