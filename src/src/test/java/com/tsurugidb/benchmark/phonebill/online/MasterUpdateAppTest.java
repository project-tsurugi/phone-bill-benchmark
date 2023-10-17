package com.tsurugidb.benchmark.phonebill.online;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.AbstractJdbcTestCase;
import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.online.MasterUpdateApp.Updater;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;
import com.tsurugidb.benchmark.phonebill.testdata.TestDataGenerator;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;
import com.tsurugidb.benchmark.phonebill.util.TestRandom;

class MasterUpdateAppTest extends AbstractJdbcTestCase {

    @Test
    void testExec() throws IOException, Exception {
        // テーブルにテストデータを入れる
        Config config = Config.getConfig();
        new CreateTable().execute(config);
        int seed = config.randomSeed;
        ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
        TestDataGenerator generator = new TestDataGenerator(config, new Random(seed), accessor);
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            generator.generateContractsToDb(manager);
        }
        List<Contract> expected = getContracts();

        // テスト用のオンラインアプリケーションを使用してアプリケーションを初期化する
        TestRandom random = new TestRandom();
        random.setValues(expected.stream().map(c -> Integer.valueOf(0)).collect(Collectors.toList()));
        RandomKeySelector<Contract.Key> keySelector = new RandomKeySelector<>(
                expected.stream().map(c -> c.getKey()).collect(Collectors.toList()),
                random, 0d
                );
        MasterUpdateApp app = new MasterUpdateApp(config, random, keySelector);
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {

            // 0番目の契約の契約終了日を契約完了日の3日後にする
            setRandomValues(random, 0, 0, 3);
            app.exec(manager);
            setEndDate(expected.get(0), 3);
            testContracts(expected);

            // 0番目の契約の契約終了日をnullにする
            setRandomValues(random, 0, 0, 0);
            app.exec(manager);
            expected.get(0).setEndDate((Date) null);
            testContracts(expected);

            // 13番目の契約の契約終了日をnullにする
            setRandomValues(random, 13, 0, 0);
            app.exec(manager);
            expected.get(13).setEndDate((Date) null);
            testContracts(expected);

            // 15番目の契約の契約終了日を契約完了日の3日後にする
            setRandomValues(random, 15, 0, 3);
            app.exec(manager);
            setEndDate(expected.get(15), 3);
            testContracts(expected);

            // 同一の電話番号を２つもつ契約の更新
            //
            // n | phone_number | start_date | end_date
            // 80 | 00000000081 | 2010-11-11 | 2017-11-12
            // 81 | 00000000081 | 2020-08-08 |
            //
            // テストデータが想定通りの値であることを確認
            assertEquals("00000000081", expected.get(80).getPhoneNumber());
            assertEquals("00000000081", expected.get(81).getPhoneNumber());
            assertEquals(DateUtils.toDate("2010-11-11"), expected.get(80).getStartDate());
            assertEquals(DateUtils.toDate("2020-08-08"), expected.get(81).getStartDate());
            assertEquals(DateUtils.toDate("2017-11-12"), expected.get(80).getEndDate());
            assertNull(expected.get(81).getEndDate());

            setRandomValues(random, 80, 0, 3);
            app.exec(manager);
            setEndDate(expected.get(80), 3);
            testContracts(expected);

            setRandomValues(random, 80, 1, 4);
            app.exec(manager);
            setEndDate(expected.get(81), 4);
            testContracts(expected);

            setRandomValues(random, 81, 0, 5);
            app.exec(manager);
            setEndDate(expected.get(80), 5);
            testContracts(expected);

            setRandomValues(random, 81, 1, 6);
            app.exec(manager);
            setEndDate(expected.get(81), 6);
            testContracts(expected);

            // 同一の電話番号で契約期間が重複しない契約がみつからずエラーになるケース
            List<Integer> list = new ArrayList<Integer>();
            list.addAll(Arrays.asList(0, 81));
            for (int i = 0; i < 100; i++) {
                list.addAll(Arrays.asList(0, 1, 3650));
            }
            random.setValues(list.toArray(new Integer[0]));

            assertThrows(RuntimeException.class, () -> app.exec(manager));
            testContracts(expected); // 値が変化していないことを確認する
        }
    }


    /**
     * n番目の契約の契約終了日が指定の値になるように乱数生成器のスタブに値をセットする。
     *
     * @param random 使用する乱数生成器
     * @param n1 何番目の契約か
     * @param n2 同一の電話番号の契約kのういち何番目の契約を更新対象にするのか
     * @param days -> 契約終了日を契約開始日の何日後にするのか、0を指定した場合契約終了日を削除する
     */
    private void setRandomValues(TestRandom random, int n1, int n2, int days) {
        if (days == 0) {
            random.setValues(n1, n2, 0);
        } else {
            random.setValues(n1, n2, 1, days);
        }
    }


    /**
     * 指定の契約の終了日を開始日のdays後に設定する
     *
     * @param days
     */
    private void setEndDate(Contract c, int days) {
        c.setEndDate(new Date(c.getStartDate().getTime() + days * DateUtils.A_DAY_IN_MILLISECONDS));
    }


    private void testContracts(List<Contract> expected) throws SQLException {
        List<Contract> actual = getContracts();
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), actual.get(i));
        }
    }


    @Test
    void testCommonDuration() {
        Contract c1 = new Contract();
        Contract c2 = new Contract();
        List<Contract> contracts = Arrays.asList(c2);

        ///////////////////////////////////////////////////// contractsの要素数が1のとき
        c1.setStartDate(DateUtils.toDate("2018-01-01"));
        c1.setEndDate(DateUtils.toDate("2018-05-01"));

        // C1の期間にC2の期間が含まれるケース
        c2.setStartDate(DateUtils.toDate("2018-03-10"));
        c2.setEndDate(DateUtils.toDate("2018-04-01"));
        assertTrue(MasterUpdateApp.commonDuration(c1, contracts));


        // C2の期間にC1の期間が含まれるケース
        c2.setStartDate(DateUtils.toDate("2017-01-10"));
        c2.setEndDate(DateUtils.toDate("2019-05-01"));
        assertTrue(MasterUpdateApp.commonDuration(c1, contracts));


        // c1 < c2 のケース
        c2.setStartDate(DateUtils.toDate("2019-01-10"));
        c2.setEndDate(DateUtils.toDate("2019-05-01"));
        assertFalse(MasterUpdateApp.commonDuration(c1, contracts));

        // c1の終了月とc2の開始月が連続する月になるケース(c1の開始日の方がc2の開始日より早い)
        c2.setStartDate(DateUtils.toDate("2018-06-01"));
        c2.setEndDate(DateUtils.toDate("2019-05-01"));
        assertFalse(MasterUpdateApp.commonDuration(c1, contracts));


        // c1の終了月とc2の開始月が一致するケース
        c2.setStartDate(DateUtils.toDate("2018-05-01"));
        c2.setEndDate(DateUtils.toDate("2019-05-01"));
        assertTrue(MasterUpdateApp.commonDuration(c1, contracts));

        c2.setStartDate(DateUtils.toDate("2018-05-31"));
        c2.setEndDate(DateUtils.toDate("2019-05-01"));
        assertTrue(MasterUpdateApp.commonDuration(c1, contracts));

        // c1の一部の期間とc2の一部の期間がかぶるとき(c1の開始日の方がc2の開始日より早い)
        c2.setStartDate(DateUtils.toDate("2018-03-01"));
        c2.setEndDate(DateUtils.toDate("2019-05-01"));
        assertTrue(MasterUpdateApp.commonDuration(c1, contracts));

        // c1の一部の期間とc2の一部の期間がかぶるとき(c1の開始日の方がc2の開始日より遅い)
        c2.setStartDate(DateUtils.toDate("2017-06-01"));
        c2.setEndDate(DateUtils.toDate("2018-02-31"));
        assertTrue(MasterUpdateApp.commonDuration(c1, contracts));

        // c1の開始月とc2の終了月が一致するケース
        c2.setStartDate(DateUtils.toDate("2017-06-01"));
        c2.setEndDate(DateUtils.toDate("2018-01-31"));
        assertTrue(MasterUpdateApp.commonDuration(c1, contracts));

        c2.setStartDate(DateUtils.toDate("2017-06-01"));
        c2.setEndDate(DateUtils.toDate("2018-01-01"));
        assertTrue(MasterUpdateApp.commonDuration(c1, contracts));

        // c1の終了月とc2の開始月が連続する月になるケース(c1の開始日の方がc2の開始日より遅い)
        c2.setStartDate(DateUtils.toDate("2017-06-01"));
        c2.setEndDate(DateUtils.toDate("2017-12-31"));
        assertFalse(MasterUpdateApp.commonDuration(c1, contracts));

        // c2 < c1 のケース
        c2.setStartDate(DateUtils.toDate("2015-01-10"));
        c2.setEndDate(DateUtils.toDate("2015-05-01"));
        assertFalse(MasterUpdateApp.commonDuration(c1, contracts));

        // c2のend_dateがnullで、期間の重複があるケース
        c2.setStartDate(DateUtils.toDate("2015-01-10"));
        c2.setEndDate((Date)null);
        assertTrue(MasterUpdateApp.commonDuration(c1, contracts));



        // c2のend_dateがnullで、期間の重複がないケース
        c2.setStartDate(DateUtils.toDate("2019-01-10"));
        c2.setEndDate((Date)null);
        assertFalse(MasterUpdateApp.commonDuration(c1, contracts));

        // c1のend_dateがnullで、期間の重複があるケース
        c1.setStartDate(DateUtils.toDate("2013-01-01"));
        c1.setEndDate((Date)null);
        c2.setStartDate(DateUtils.toDate("2015-01-10"));
        c2.setEndDate(DateUtils.toDate("2015-05-01"));
        assertTrue(MasterUpdateApp.commonDuration(c1, contracts));

        // c1のend_dateがnullで、期間の重複がないケース
        c2.setStartDate(DateUtils.toDate("2011-01-10"));
        c2.setEndDate(DateUtils.toDate("2011-05-01"));
        assertFalse(MasterUpdateApp.commonDuration(c1, contracts));

        // c1,c2のend_dateがともにnullで、c1.start_date < c2_start_dateのケース
        c1.setStartDate(DateUtils.toDate("2013-01-01"));
        c1.setEndDate((Date)null);
        c2.setStartDate(DateUtils.toDate("2015-01-10"));
        c2.setEndDate((Date)null);
        assertTrue(MasterUpdateApp.commonDuration(c1, contracts));

        // c1,c2のend_dateがともにnullで、c1.start_date < c2_start_dateのケース
        c1.setStartDate(DateUtils.toDate("2015-01-01"));
        c1.setEndDate((Date)null);
        c2.setStartDate(DateUtils.toDate("2013-01-10"));
        c2.setEndDate((Date)null);
        assertTrue(MasterUpdateApp.commonDuration(c1, contracts));


        // c1,c2のend_dateがともにnullで、c1.start_date - c2_start_dateのケース
        c1.setStartDate(DateUtils.toDate("2013-01-01"));
        c1.setEndDate((Date)null);
        c2.setStartDate(DateUtils.toDate("2013-01-10"));
        c2.setEndDate((Date)null);
        assertTrue(MasterUpdateApp.commonDuration(c1, contracts));


        ///////////////////////////////////////////////////// contractsに複数の要素があるケース

        Contract c3 = new Contract();
        Contract c4 = new Contract();
        contracts = Arrays.asList(c2, c3, c4);


        // 重複期間が一つもないケース
        c1.setStartDate(DateUtils.toDate("2018-01-01"));
        c1.setEndDate(DateUtils.toDate("2018-05-01"));
        c2.setStartDate(DateUtils.toDate("2019-03-10"));
        c2.setEndDate(DateUtils.toDate("2019-04-01"));
        c3.setStartDate(DateUtils.toDate("2020-03-10"));
        c3.setEndDate(DateUtils.toDate("2020-04-01"));
        c4.setStartDate(DateUtils.toDate("2021-03-10"));
        c4.setEndDate(DateUtils.toDate("2021-04-01"));
        assertFalse(MasterUpdateApp.commonDuration(c1, contracts));


        // 期間が重複する契約が一つだけあるケース
        c1.setStartDate(DateUtils.toDate("2019-01-01"));
        c1.setEndDate(DateUtils.toDate("2019-12-01"));
        assertTrue(MasterUpdateApp.commonDuration(c1, contracts));


        // すべての契約の期間が重複するケース
        c1.setStartDate(DateUtils.toDate("2000-01-01"));
        c1.setEndDate(DateUtils.toDate("2119-12-01"));
        assertTrue(MasterUpdateApp.commonDuration(c1, contracts));
    }


    @Test
    void testUpdater1() throws SQLException, IOException {
        RandomKeySelector<Key> keySelector = new RandomKeySelector<>(Collections.emptyList(), new Random(), 0d);
        MasterUpdateApp app = new MasterUpdateApp(Config.getConfig(), new Random(), keySelector);
        Updater updater = app.new Updater1();

        Contract contract = new Contract();
        contract.setEndDate(DateUtils.toDate("2020-02-22"));
        updater.update(contract);
        assertNull(contract.getEndDate());
    }

    @Test
    void testUpdater2() throws IOException, SQLException {
        RandomKeySelector<Key> keySelector = new RandomKeySelector<>(Collections.emptyList(), new Random(), 0d);

        Config config = Config.getConfig();
        config.minDate = DateUtils.toDate("2010-12-15");
        config.maxDate = DateUtils.toDate("2020-02-15");
        MasterUpdateApp app = new MasterUpdateApp(config, new Random(), keySelector);
        Updater updater = app.new Updater2();

        // ContractのstartDateと、ConfigのmaxDateが等しい場合、Contract.endDateも同じ値になる
        for (int i = 0; i < 100; i++) {
            Contract contract = new Contract();
            contract.setStartDate(config.maxDate);
            contract.setEndDate((Date)null);
            updater.update(contract);
            assertEquals(DateUtils.toDate("2020-02-15"), contract.getEndDate());
        }

        // Contract.endDateが、Contract.startDate と Config.maxDateの間の値になることを確認
        Set<Date> actual = new HashSet<Date>();
        Set<Date> expected = new HashSet<Date>();
        expected.add(DateUtils.toDate("2020-02-13"));
        expected.add(DateUtils.toDate("2020-02-14"));
        expected.add(DateUtils.toDate("2020-02-15"));

        for (int i = 0; i < 100; i++) {
            Contract contract = new Contract();
            contract.setStartDate(DateUtils.toDate("2020-02-13"));
            contract.setEndDate((Date)null);
            updater.update(contract);
            actual.add(contract.getEndDate());
        }
        assertEquals(expected, actual);
    }
}
