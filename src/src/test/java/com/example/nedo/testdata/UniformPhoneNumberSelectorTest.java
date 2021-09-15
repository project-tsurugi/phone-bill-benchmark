package com.example.nedo.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.example.nedo.app.Config;
import com.example.nedo.db.DBUtils;
import com.example.nedo.db.Duration;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

class UniformPhoneNumberSelectorTest {

	private static final int NUMBER_OF_CONTRACTS_RECORDS = 1000;
	private MyRandom random = new MyRandom();
	private UniformPhoneNumberSelector selector = null;
	private TestDataGenerator generator = null;

	/**
	 * selectPhoneNumber()のテスト
	 * @throws IOException
	 */
	@Test
	void testSelectContract() throws IOException {
		Config config = Config.getConfig();
		config.expirationDateRate =4;
		config.noExpirationDateRate = 0;
		config.duplicatePhoneNumberRatio = 0;
		config.minDate = DBUtils.toDate("2010-01-11");
		config.maxDate = DBUtils.toDate("2020-12-21");
		config.numberOfContractsRecords = NUMBER_OF_CONTRACTS_RECORDS;
		generator = new TestDataGenerator(config);

		// 契約期間をテスト用の値に書き換える
		List<Duration> list = generator.getDurationList();
		assertEquals(4, list.size()); // 要素数が想定通りか確認
		list.get(0).start = config.minDate.getTime();
		list.get(1).start = config.minDate.getTime();
		list.get(2).start = config.minDate.getTime();
		list.get(3).start = config.minDate.getTime();
		list.get(0).end = DBUtils.toDate("2010-02-11").getTime();
		list.get(1).end = DBUtils.toDate("2010-03-11").getTime();
		list.get(2).end = null;
		list.get(3).end = DBUtils.toDate("2010-04-11").getTime();


		for(Duration d: list) {
			System.out.println(""  + d.start +","+ d.end);
		}

		ContractReader contractReader = generator.new ContractReaderImpl();
		selector = new UniformPhoneNumberSelector(random, contractReader, generator.getDurationList().size());

		// 指定されたstartPosをそのまま返すケース

		testSelectContract(400, "2010-01-13", -1, 400);
		testSelectContract(401, "2010-01-13", -1, 401);
		testSelectContract(402, "2010-01-13", -1, 402);
		testSelectContract(403, "2010-01-13", -1, 403);

		// 契約期間がマッチするまで探すケース
		testSelectContract(402, "2010-03-13", -1, 400);
		testSelectContract(402, "2010-03-13", -1, 401);
		testSelectContract(402, "2010-03-13", -1, 402);
		testSelectContract(403, "2010-03-13", -1, 403);

		testSelectContract(402, "2011-03-13", -1, 402);
		testSelectContract(406, "2011-03-13", -1, 403);
		testSelectContract(406, "2011-03-13", -1, 404);
		testSelectContract(406, "2011-03-13", -1, 405);

		// 契約番号が一巡するケース
		testSelectContract(2, "2011-03-13", -1, NUMBER_OF_CONTRACTS_RECORDS - 1);

		// startTimeが不正で、対応する契約が見つからないケース
		Exception e1 = assertThrows(RuntimeException.class, () -> testSelectContract(0, "2001-01-13", -1, 400));
		assertTrue(e1.getMessage().startsWith("Not found!"));
		Exception e2 = assertThrows(RuntimeException.class, () -> testSelectContract(0, "2012-01-11", 402, 400));
		assertTrue(e2.getMessage().startsWith("Not found!"));

	}

	/**
	 * 引数にDateStringで表される通話開始時刻、ecept番目の除外電話番号を引数にselectPhoneNumber()を
	 * 呼び出したときに、testSelectContract()が返す電話番号がexpect番目の電話番号であることを確認する。
	 * selectPhoneNumber()は乱数で決定する電話番号は、テスト用の乱数生成器を用いて、choose番目の電話番号
	 * になる。
	 *
	 * @param expect 期待する電話番号
	 * @param DateString 通話開始時刻
	 * @param except 除外電話番号、除外電話番号を指定しない場合は-1
	 * @param choose 乱数により選択される電話番号
	 */
	void testSelectContract(int expect, String DateString, int except, int choose) {
		random.setNextValue(((double)choose)/NUMBER_OF_CONTRACTS_RECORDS);
		assertEquals(choose, TestDataUtils.getRandomLong(random, 0, NUMBER_OF_CONTRACTS_RECORDS)); // 乱数発生器が想定した値を返すことを確認
		long startTime = DBUtils.toDate(DateString).getTime();
		assertEquals(expect, selector.selectPhoneNumber(startTime, except));
	}

	// テスト用のrandomクラス
	@SuppressFBWarnings("SE_NO_SERIALVERSIONID")
	static class MyRandom extends Random {
		double nextValue;

		@Override
		public double nextDouble() {
			return nextValue;
		}

		/**
		 * @param nextValue セットする nextValue
		 */
		public void setNextValue(double nextValue) {
			this.nextValue = nextValue;
		}
	};

}
