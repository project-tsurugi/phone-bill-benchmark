/**
 *
 */
package com.tsurugidb.benchmark.phonebill.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.jdbc.Duration;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;
import com.tsurugidb.benchmark.phonebill.util.TestRandom;

class ContractInfoReaderTest {
    private static final Date DATE01 = DateUtils.toDate("2020-01-01");
    private static final Date DATE02 = DateUtils.toDate("2021-08-15");
    private static final Date DATE03 = DateUtils.toDate("2010-01-01");
    private static final Date DATE04 = DateUtils.toDate("2016-01-27");

    /**
     * ContractBlockInfoAccessorのテスト用スタブ
     */
    private ContractBlockInfoAccessorStub accessor = new ContractBlockInfoAccessorStub();

    /**
     * テスト用のRandom
     */
    private TestRandom random = new TestRandom();



    /**
     * テスト用のContractInfoReader、setUp()で初期化される
     */
    private ContractInfoReader contractInfoReader;

    @BeforeEach
    void setUp() throws Exception {
        PhoneNumberGenerator phoneNumberGenerator = new PhoneNumberGenerator(Config.getConfig());
        List<Duration> durationList = new ArrayList<>();
        durationList.add(new Duration(DATE01, null));
        durationList.add(new Duration(DATE01, DATE02));
        durationList.add(new Duration(DATE03, DATE04));
        List<Boolean> statusList = new ArrayList<>();
        statusList.add(true);
        statusList.add(false);
        statusList.add(false);
        accessor = new ContractBlockInfoAccessorStub(1, 5, 7, 2, 6);
        contractInfoReader = new ContractInfoReader(durationList, statusList, accessor,
                phoneNumberGenerator, random);
    }


    @Test
    void testConstractor() throws IOException {
        PhoneNumberGenerator phoneNumberGenerator = new PhoneNumberGenerator(Config.getConfig());
        List<Duration> durationList = new ArrayList<>();
        durationList.add(new Duration(DATE01, null));
        durationList.add(new Duration(DATE01, DATE02));
        durationList.add(new Duration(DATE03, DATE04));
        List<Boolean> statusList = new ArrayList<>();
        statusList.add(true);
        statusList.add(false);
        statusList.add(false);
        accessor = new ContractBlockInfoAccessorStub(1, 5, 9, 2, 7, 3, 4, 8, 0);
        ContractInfoReader target = new ContractInfoReader(durationList, statusList, accessor,
                phoneNumberGenerator, random);


        assertEquals(3, target.getBlockSize());
        assertEquals(Arrays.asList(7, 8, 9), target.getBlockInfos().getActiveBlocks());
        assertEquals(9, target.getBlockInfos().getNumberOfActiveBlacks());
        assertEquals(5, target.getBlockInfos().getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

        // リストのサイズが違うとExceptionがスローされる
        statusList.add(false);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new ContractInfoReader(durationList, statusList,
                accessor, phoneNumberGenerator, random));
        assertEquals("Array size mismatch.", e.getMessage());
    }

    @Test
    void testLoadActiveBlockNumberList() throws IOException {
        // 空のリスト
        accessor.setBlockNumbers();
        contractInfoReader.loadActiveBlockNumberList();
        assertEquals(Collections.emptyList(), contractInfoReader.getBlockInfos().getActiveBlocks());
        assertEquals(0, contractInfoReader.getBlockInfos().getNumberOfActiveBlacks());
        assertEquals(-1, contractInfoReader.getBlockInfos().getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

        // 要素数が1で値が0
        accessor.setBlockNumbers(0);
        contractInfoReader.loadActiveBlockNumberList();
        assertEquals(Collections.emptyList(), contractInfoReader.getBlockInfos().getActiveBlocks());
        assertEquals(1, contractInfoReader.getBlockInfos().getNumberOfActiveBlacks());
        assertEquals(0, contractInfoReader.getBlockInfos().getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

        // 要素数が1で値が0以外
        accessor.setBlockNumbers(2);
        contractInfoReader.loadActiveBlockNumberList();
        assertEquals(Arrays.asList(2), contractInfoReader.getBlockInfos().getActiveBlocks());
        assertEquals(1, contractInfoReader.getBlockInfos().getNumberOfActiveBlacks());
        assertEquals(-1, contractInfoReader.getBlockInfos().getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

        // 先頭部の省略あり
        accessor.setBlockNumbers(5, 0, 1, 2, 8);
        contractInfoReader.loadActiveBlockNumberList();
        assertEquals(Arrays.asList(5, 8), contractInfoReader.getBlockInfos().getActiveBlocks());
        assertEquals(5, contractInfoReader.getBlockInfos().getNumberOfActiveBlacks());
        assertEquals(2, contractInfoReader.getBlockInfos().getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

        // 先頭部の省略なし
        accessor.setBlockNumbers(0, 7, 6, 4);
        contractInfoReader.loadActiveBlockNumberList();
        assertEquals(Arrays.asList(4, 6, 7), contractInfoReader.getBlockInfos().getActiveBlocks());
        assertEquals(4, contractInfoReader.getBlockInfos().getNumberOfActiveBlacks());
        assertEquals(0, contractInfoReader.getBlockInfos().getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

        // 0から始まらないケース
        accessor.setBlockNumbers(3, 7, 2, 9);
        contractInfoReader.loadActiveBlockNumberList();
        assertEquals(Arrays.asList(2, 3, 7, 9), contractInfoReader.getBlockInfos().getActiveBlocks());
        assertEquals(4, contractInfoReader.getBlockInfos().getNumberOfActiveBlacks());
        assertEquals(-1, contractInfoReader.getBlockInfos().getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

        // ブロック番号が完全に連続なケース
        accessor.setBlockNumbers(0, 1, 2, 3, 4, 5);
        contractInfoReader.loadActiveBlockNumberList();
        assertEquals(Collections.emptyList(), contractInfoReader.getBlockInfos().getActiveBlocks());
        assertEquals(6, contractInfoReader.getBlockInfos().getNumberOfActiveBlacks());
        assertEquals(5, contractInfoReader.getBlockInfos().getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
    }

    @Test
    void testGetKeyNewContract() throws IOException {
        PhoneNumberGenerator g = new PhoneNumberGenerator(Config.getConfig());
        Contract c;
        // アクティブなブロックの最大値が7なので、新しいブロックのブロック番号は8になる
        c = contractInfoReader.getNewContract();
        assertEquals(g.getPhoneNumber(24), c.getPhoneNumber());
        assertEquals(DATE01, c.getStartDate());
        assertEquals(null, c.getEndDate());
        assertEquals("dummy", c.getRule());

        c = contractInfoReader.getNewContract();
        assertEquals(g.getPhoneNumber(25), c.getPhoneNumber());
        assertEquals(DATE01, c.getStartDate());
        assertEquals(DATE02, c.getEndDate());
        assertEquals("dummy", c.getRule());

        c = contractInfoReader.getNewContract();
        assertEquals(g.getPhoneNumber(26), c.getPhoneNumber());
        assertEquals(DATE03, c.getStartDate());
        assertEquals(DATE04, c.getEndDate());
        assertEquals("dummy", c.getRule());

        c = contractInfoReader.getNewContract();
        assertEquals(g.getPhoneNumber(27), c.getPhoneNumber());
        assertEquals(DATE01, c.getStartDate());
        assertEquals(null, c.getEndDate());
        assertEquals("dummy", c.getRule());

        c = contractInfoReader.getNewContract();
        assertEquals(g.getPhoneNumber(28), c.getPhoneNumber());
        assertEquals(DATE01, c.getStartDate());
        assertEquals(DATE02, c.getEndDate());
        assertEquals("dummy", c.getRule());

        c = contractInfoReader.getNewContract();
        assertEquals(g.getPhoneNumber(29), c.getPhoneNumber());
        assertEquals(DATE03, c.getStartDate());
        assertEquals(DATE04, c.getEndDate());
        assertEquals("dummy", c.getRule());

        c = contractInfoReader.getNewContract();
        assertEquals(g.getPhoneNumber(30), c.getPhoneNumber());
        assertEquals(DATE01, c.getStartDate());
        assertEquals(null, c.getEndDate());
        assertEquals("dummy", c.getRule());

    }

    @Test
    void testGetKeyUpdatingContract() throws IOException {
        random.setValues(
                0, 0, // block = 1, n = 3
                4, 1, // block = 7, n = 22
                2, 0, // block = 5, n = 15
                2, 2, // block = 5, n = 17
                2, 0, // block = 5, n = 15
                4, 2); // block =7, n = 23
        PhoneNumberGenerator g = new PhoneNumberGenerator(Config.getConfig());
        Key key;
        // アクティブなブロックの最大値が7なので、新しいブロックのブロック番号は8になる
        // 1ブロックのサイズが3なので、n = 3 * 8 = 24のキーが生成される
        key = contractInfoReader.getKeyUpdatingContract();
        assertEquals(g.getPhoneNumber(3), key.getPhoneNumber());
        assertEquals(DATE01, key.getStartDate());

        key = contractInfoReader.getKeyUpdatingContract();
        assertEquals(g.getPhoneNumber(22), key.getPhoneNumber());
        assertEquals(DATE01, key.getStartDate());

        key = contractInfoReader.getKeyUpdatingContract();
        assertEquals(g.getPhoneNumber(15), key.getPhoneNumber());
        assertEquals(DATE01, key.getStartDate());

        key = contractInfoReader.getKeyUpdatingContract();
        assertEquals(g.getPhoneNumber(17), key.getPhoneNumber());
        assertEquals(DATE03, key.getStartDate());

        key = contractInfoReader.getKeyUpdatingContract();
        assertEquals(g.getPhoneNumber(15), key.getPhoneNumber());
        assertEquals(DATE01, key.getStartDate());

        key = contractInfoReader.getKeyUpdatingContract();
        assertEquals(g.getPhoneNumber(23), key.getPhoneNumber());
        assertEquals(DATE03, key.getStartDate());
    }

    @Test
    void testIsActive() {
        assertEquals(true, contractInfoReader.isActive(0));
        assertEquals(false, contractInfoReader.isActive(1));
        assertEquals(false, contractInfoReader.isActive(2));

        assertEquals(true, contractInfoReader.isActive(3));
        assertEquals(false, contractInfoReader.isActive(4));
        assertEquals(false, contractInfoReader.isActive(5));

        assertEquals(true, contractInfoReader.isActive(5181));
        assertEquals(true, contractInfoReader.isActive(3162));
        assertEquals(true, contractInfoReader.isActive(4401));

        assertEquals(false, contractInfoReader.isActive(101));
        assertEquals(false, contractInfoReader.isActive(51553));
        assertEquals(false, contractInfoReader.isActive(190523));
    }

    @Test
    void testGetInitialDuration() {
        assertEquals(new Duration(DATE01, null), contractInfoReader.getInitialDuration(0));
        assertEquals(new Duration(DATE01, DATE02), contractInfoReader.getInitialDuration(1));
        assertEquals(new Duration(DATE03, DATE04), contractInfoReader.getInitialDuration(2));
        assertEquals(new Duration(DATE01, null), contractInfoReader.getInitialDuration(3));
        assertEquals(new Duration(DATE01, DATE02), contractInfoReader.getInitialDuration(4));
        assertEquals(new Duration(DATE03, DATE04), contractInfoReader.getInitialDuration(5));
        assertEquals(new Duration(DATE01, null), contractInfoReader.getInitialDuration(6));
    }

    @Test
    void testGetRandomBlockNumber() throws IOException {
        // ブロック番号が連続なケース
        random.setValues(0, 1, 2, 3);
        accessor.setBlockNumbers(0, 1, 2, 3);
        contractInfoReader.loadActiveBlockNumberList();
        assertEquals(0, contractInfoReader.getRandomBlockNumber());
        assertEquals(1, contractInfoReader.getRandomBlockNumber());
        assertEquals(2, contractInfoReader.getRandomBlockNumber());
        assertEquals(3, contractInfoReader.getRandomBlockNumber());

        // ブロック番号が0から始まらないケース
        random.setValues(0, 1, 2, 3);
        accessor.setBlockNumbers(3, 9, 6, 4);
        contractInfoReader.loadActiveBlockNumberList();
        assertEquals(3, contractInfoReader.getRandomBlockNumber());
        assertEquals(4, contractInfoReader.getRandomBlockNumber());
        assertEquals(6, contractInfoReader.getRandomBlockNumber());
        assertEquals(9, contractInfoReader.getRandomBlockNumber());

        // 不連続なブロック番号が0から始まるケース
        random.setValues(0, 1, 2, 3);
        accessor.setBlockNumbers(0, 9, 6, 1);
        contractInfoReader.loadActiveBlockNumberList();
        assertEquals(0, contractInfoReader.getRandomBlockNumber());
        assertEquals(1, contractInfoReader.getRandomBlockNumber());
        assertEquals(6, contractInfoReader.getRandomBlockNumber());
        assertEquals(9, contractInfoReader.getRandomBlockNumber());
    }

    /**
     * ContractBlockInfoAccessorのテスト用スタブ
     * <p>
     * setBlockNumbers()を使用して、保持する契約のブロック番号のリストを変更できる。
     */
    private static class ContractBlockInfoAccessorStub implements ContractBlockInfoAccessor {
        private ActiveBlockNumberHolder blockInfos = new ActiveBlockNumberHolder();

        public ContractBlockInfoAccessorStub(Integer... blockNumber) {
            setBlockNumbers(blockNumber);
        }

        public void setBlockNumbers(Integer... blockNumber) {
            blockInfos.setActiveBlocks(Arrays.asList(blockNumber));
        }

        @Override
        public int getNewBlock() {
            List<Integer> list = blockInfos.getActiveBlocks();
            int n = list.get(list.size() - 1) + 1;
            return n;
        }

        @Override
        public void submit(int blockNumber) {
            blockInfos.addActiveBlockNumber(blockNumber);
            return;
        }

        @Override
        public ActiveBlockNumberHolder getActiveBlockInfo() {
            return blockInfos;
        }
    }

    /**
     * getDate()で得られる値が、start ～ endの範囲に収まることのテスト
     * @throws IOException
     */
    @Test
    void tesGetDate3() throws IOException {
        Date start = DateUtils.toDate("2020-11-30");
        Date end = DateUtils.toDate("2020-12-02");
        Set<Date> expected = new TreeSet<>(Arrays.asList(
                DateUtils.toDate("2020-11-30"),
                DateUtils.toDate("2020-12-01"),
                DateUtils.toDate("2020-12-02")));
        Set<Date> actual = new TreeSet<>();

        Config config = Config.getConfig();
        config.minDate = start;
        config.maxDate = end;
        for(int i = 0; i < 100; i++) {
            actual.add(ContractInfoReader.getDate(start, end, new Random()));
        }
        assertEquals(expected, actual);
    }

    /**
     * getDate()で得られる値が、start ～ endの範囲に収まることのテスト
     * @throws IOException
     */
    @Test
    void tesGetDate7() throws IOException {
        Date start = DateUtils.toDate("2020-11-30");
        Date end = DateUtils.toDate("2020-12-06");
        Set<Date> expected = new TreeSet<>(Arrays.asList(
                DateUtils.toDate("2020-11-30"),
                DateUtils.toDate("2020-12-01"),
                DateUtils.toDate("2020-12-02"),
                DateUtils.toDate("2020-12-03"),
                DateUtils.toDate("2020-12-04"),
                DateUtils.toDate("2020-12-05"),
                DateUtils.toDate("2020-12-06")));
        Set<Date> actual = new TreeSet<>();

        Config config = Config.getConfig();
        config.minDate = start;
        config.maxDate = end;
        for(int i = 0; i < 100; i++) {
            actual.add(ContractInfoReader.getDate(start, end, new Random()));
        }
        assertEquals(expected, actual);
    }


    /**
     * initDurationList()のテスト
     * @throws IOException
     */
    @Test
    void testInitDurationList() throws IOException {
        // 通常ケース
        testInitDurationLisSub(1, 3, 7, DateUtils.toDate("2010-11-11"), DateUtils.toDate("2020-01-01"));
        testInitDurationLisSub(13, 5, 2, DateUtils.toDate("2010-11-11"), DateUtils.toDate("2020-01-01"));
        // 一項目が0
        testInitDurationLisSub(3, 7, 0, DateUtils.toDate("2010-11-11"), DateUtils.toDate("2020-01-01"));
        testInitDurationLisSub(3, 0, 5, DateUtils.toDate("2010-11-11"), DateUtils.toDate("2020-01-01"));
        testInitDurationLisSub(0, 7, 5, DateUtils.toDate("2010-11-11"), DateUtils.toDate("2020-01-01"));
        // 二項目が0
        testInitDurationLisSub(0, 7, 0, DateUtils.toDate("2010-11-11"), DateUtils.toDate("2020-01-01"));
        testInitDurationLisSub(3, 0, 0, DateUtils.toDate("2010-11-11"), DateUtils.toDate("2020-01-01"));
        testInitDurationLisSub(0, 0, 5, DateUtils.toDate("2010-11-11"), DateUtils.toDate("2020-01-01"));
        // startの翌日=endのケース
        testInitDurationLisSub(13, 5, 2, DateUtils.toDate("2019-12-31"), DateUtils.toDate("2020-01-01"));

    }

    void testInitDurationLisSub(int duplicatePhoneNumberRate, int expirationDateRate, int noExpirationDateRate
        , Date start, Date end) throws IOException {
        Config config = Config.getConfig();
        config.duplicatePhoneNumberRate = duplicatePhoneNumberRate;
        config.expirationDateRate = expirationDateRate;
        config.noExpirationDateRate = noExpirationDateRate;
        config.minDate = start;
        config.maxDate = end;

        List<Duration> list = ContractInfoReader.initDurationList(config);
        // listの要素数が duplicatePhoneNumberRate * 2 + expirationDateRate + noExpirationDateRateであること
        assertEquals(duplicatePhoneNumberRate * 2 + expirationDateRate + noExpirationDateRate, list.size());
        // 始めの、expirationDateRate + noExpirationDateRate 個の要素を調べると、契約終了日が存在する要素数が
        // expirationDateRate, 契約終了日が存在しない要数がnoExpirationDateRateであること。
        int n1 = 0;
        int n2 = 0;
        for(int i = 0; i < expirationDateRate + noExpirationDateRate; i++) {
            Duration d = list.get(i);
            assertNotNull(d.start);
            if (d.end == null) {
                n1++;
            } else {
                n2++;
            }
        }
        assertEquals(noExpirationDateRate, n1);
        assertEquals(expirationDateRate, n2);
        // expirationDateRate + noExpirationDateRateより後の要素は以下の2つの要素のペアが、duplicatePhoneNumberRatio個
        // 続いていること。
        //
        // 1番目の要素の、startがContractsGeneratorのコンストラクタに渡したstartと等しい
        // 2番目の要素のendがnull
        // 2番目の要素のstartが1番目の要素のendより大きい

        for(int i = expirationDateRate + noExpirationDateRate; i < list.size(); i+=2) {
            Duration d1 = list.get(i);
            Duration d2 = list.get(i+1);
            assertEquals(start, d1.getStatDate());
            assertTrue(d1.end < d2.start);
            assertNull(d2.end);
        }
    }
}
