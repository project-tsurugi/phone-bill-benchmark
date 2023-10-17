package com.tsurugidb.benchmark.phonebill.online;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeTestTools;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;
import com.tsurugidb.benchmark.phonebill.testdata.TestDataGenerator;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;
import com.tsurugidb.benchmark.phonebill.util.TestRandom;

class MasterUpdateAppIceaxeTest {
    private static String ICEAXE_CONFIG = "src/test/config/iceaxe.properties";

    @Test
    void testExec() throws IOException, Exception {
        // テーブルにテストデータを入れる
        Config config = Config.getConfig(ICEAXE_CONFIG);
        config.numberOfContractsRecords = 100;
        new CreateTable().execute(config);
        int seed = config.randomSeed;
        ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
        TestDataGenerator generator = new TestDataGenerator(config, new Random(seed), accessor);
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            generator.generateContractsToDb(manager);
        }

        try (IceaxeTestTools testTools = new IceaxeTestTools(config)) {
            List<Contract> expected = testTools.getContractList();

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
                testContracts(expected, testTools);

                // 0番目の契約の契約終了日をnullにする
                setRandomValues(random, 0, 0, 0);
                app.exec(manager);
                expected.get(0).setEndDate((Date) null);
                testContracts(expected, testTools);

                // 13番目の契約の契約終了日をnullにする
                setRandomValues(random, 13, 0, 0);
                app.exec(manager);
                expected.get(13).setEndDate((Date) null);
                testContracts(expected, testTools);

                // 15番目の契約の契約終了日を契約完了日の3日後にする
                setRandomValues(random, 15, 0, 3);
                app.exec(manager);
                setEndDate(expected.get(15), 3);
                testContracts(expected, testTools);

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
                testContracts(expected, testTools);

                setRandomValues(random, 80, 1, 4);
                app.exec(manager);
                setEndDate(expected.get(81), 4);
                testContracts(expected, testTools);

                setRandomValues(random, 81, 0, 5);
                app.exec(manager);
                setEndDate(expected.get(80), 5);
                testContracts(expected, testTools);

                setRandomValues(random, 81, 1, 6);
                app.exec(manager);
                setEndDate(expected.get(81), 6);
                testContracts(expected, testTools);

                // 同一の電話番号で契約期間が重複しない契約がみつからずエラーになるケース
                List<Integer> list = new ArrayList<Integer>();
                list.addAll(Arrays.asList(0, 81));
                for (int i = 0; i < 100; i++) {
                    list.addAll(Arrays.asList(0, 1, 3650));
                }
                random.setValues(list.toArray(new Integer[0]));
                assertThrows(RuntimeException.class, () -> app.exec(manager));
                testContracts(expected, testTools);
            }
        }
    }

    /**
     * n番目の契約の契約終了日が指定の値になるように乱数生成器に値をセットする。
     *
     * @param random 使用する乱数生成器
     * @param n1 何番目の契約か(この契約とおなじ電話番号をもつ契約が更新対象になる
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


    private void testContracts(List<Contract> expected, IceaxeTestTools testTools) throws SQLException {
        List<Contract> actual = testTools.getContractList();
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), actual.get(i));
        }
    }
}
