package com.tsurugidb.benchmark.phonebill.online;

import static org.junit.jupiter.api.Assertions.*;

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
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;
import com.tsurugidb.benchmark.phonebill.testdata.TestDataGenerator;
import com.tsurugidb.benchmark.phonebill.util.TestRandom;

class MasterDeleteInsertAppTest extends AbstractJdbcTestCase {

    @Test
    void test() throws Exception {
        // テーブルにテストデータを入れる
        Config config = Config.getConfig();
        new CreateTable().execute(config);
        int seed = config.randomSeed;
        ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
        TestDataGenerator generator = new TestDataGenerator(config, new Random(seed), accessor);
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            generator.generateContractsToDb(manager);
        }
        List<Contract> list = getContracts();

        // テスト用のオンラインアプリケーションを初期化する
        TestRandom random = new TestRandom();
        random.setValues(list.stream().map(c -> Integer.valueOf(0)).collect(Collectors.toList()));
        RandomKeySelector<Contract.Key> keySelector = new RandomKeySelector<>(
                list.stream().map(c -> c.getKey()).collect(Collectors.toList()),
                random, 0d
                );
        MasterDeleteInsertApp app = new MasterDeleteInsertApp(config, random, keySelector);


        random.setValues(1, 5, 3);
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            // 乱数により選択されたコードが削除される
            Set<Contract> expectedSet = new HashSet<>(list);
            expectedSet.remove(list.get(1));
            app.exec(manager);
            assertEquals(expectedSet , getContractSet());

            // 削除したレコードがinsertされて復活する
            expectedSet.add(list.get(1));
            app.exec(manager);
            assertEquals(expectedSet , getContractSet());

            // 乱数により選択されたコードが削除される
            expectedSet.remove(list.get(5));
            app.exec(manager);
            assertEquals(expectedSet , getContractSet());

            // 削除したレコードがinsertされて復活する
            expectedSet.add(list.get(5));
            app.exec(manager);
            assertEquals(expectedSet , getContractSet());

            // 乱数により選択されたコードが削除される
            expectedSet.remove(list.get(3));
            app.exec(manager);
            assertEquals(expectedSet , getContractSet());

            // 削除したレコードがinsertされて復活する
            expectedSet.add(list.get(3));
            app.exec(manager);
            assertEquals(expectedSet , getContractSet());
        }
    }
}
