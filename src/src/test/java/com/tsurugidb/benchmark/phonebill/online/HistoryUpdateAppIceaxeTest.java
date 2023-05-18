package com.tsurugidb.benchmark.phonebill.online;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeTestTools;
import com.tsurugidb.benchmark.phonebill.testdata.CreateTestData;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;
import com.tsurugidb.benchmark.phonebill.util.TestRandom;

class HistoryUpdateAppIceaxeTest {
    private static String ICEAXE_CONFIG = "src/test/config/iceaxe.properties";

    private Config config;
    private HistoryUpdateApp app;
    private TestRandom random;
    private IceaxeTestTools testTools;

    @BeforeEach
    void before() throws Exception {
        config = Config.getConfig(ICEAXE_CONFIG);
        config.numberOfContractsRecords = 10;
        config.expirationDateRate =3;
        config.noExpirationDateRate = 3;
        config.duplicatePhoneNumberRate = 2;
        config.numberOfHistoryRecords = 30;
        testTools = new IceaxeTestTools(config);

        // テストデータを入れる
        new CreateTable().execute(config);
        new CreateTestData().execute(config);

        // アプリケーションの初期化
        // アプリケーションの初期化
        random = new TestRandom();
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            List<Key> contracts = manager.execute(TxOption.of(), () -> {
                return manager.getContractDao().getAllPrimaryKeys();
            });
            Integer[] values = new Integer[contracts.size()];
            for(int i = 0; i < values.length; i++) {
                values[i] = Integer.valueOf(0);
            }
            random.setValues(values);
            RandomKeySelector<Key> keySelector = new RandomKeySelector<>(contracts, random, 0);
            app = new HistoryUpdateApp(config, random, keySelector);
        }
    }

    @AfterEach
    void after() throws SQLException {
        if (testTools != null) {
            testTools.close();
        }
    }

    @Test
    void testExec() throws Exception {
        List<History> histories = testTools.getHistoryList();
        List<Contract> contracts = testTools.getContractList();
        Map<Key, List<History>> map = getContractHistoryMap(contracts, histories);
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            // 削除フラグを立てるケース
            History target;
            target = histories.get(18);
            setRandom(contracts, map, target, true, 0);
            app.exec(manager);
            target.setDf(1);
            target.setCharge(null);
            testExecSub(histories);

            // 通話時間を更新するケース
            target = histories.get(23);
            setRandom(contracts, map, target, false, 3185);
            app.exec(manager);
            target.setTimeSecs(3185 + 1); // 通話時間は random.next() + 1 なので、
            target.setCharge(null);
            testExecSub(histories);
        }

    }

    /**
     * 指定したtargetを対象に履歴が更新されるように乱数生成器のスタブを設定する
     *
     * @param contracts 契約マスタ
     * @param map 契約マスタと当該契約に属する履歴のリストのマップ
     * @param target 更新対象の履歴
     * @param delete trueのとき論理削除、falseの時、通話時間を更新する。
     * @param timeSec 通話時間を更新するときの通話時間
     */
    private void setRandom(List<Contract> contracts, Map<Key, List<History>> map, History target, boolean delete, int timeSec) {
        int nContract = -1;
        int nHistory = -1;
        for (int i = 0; i < contracts.size(); i++) {
            Key key = contracts.get(i).getKey();
            List<History> list = map.get(key);
            for (int j = 0; j < list.size(); j++) {
                if (list.get(j) == target) {
                    nContract = i;
                    nHistory = j;
                    break;
                }
            }
            if (nContract >= 0) {
                break;
            }
        }
        if (delete) {
            random.setValues(nContract, nHistory, 0);
        } else {
            random.setValues(nContract, nHistory, 1, timeSec);
        }
    }



    private void testExecSub(List<History> expected) throws SQLException, IOException {
        List<History> actual = testTools.getHistoryList();
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), actual.get(i), "i = " + i);
        }
    }




    /**
     * すべての契約と契約に属する履歴のマップを作成する
     *
     * @return
     * @throws SQLException
     */
    Map<Key, List<History>> getContractHistoryMap(List<Contract> contracts, List<History> histories) throws SQLException {
        // 通話履歴の開始時刻との比較を簡単にするため、契約のendDateを書き換える
        contracts.stream().forEach( c -> c.setEndDate(c.getEndDate() == null ? DateUtils.toDate("2099-12-31") : DateUtils.nextDate(c.getEndDate())));
        Map<Key, List<History>> map = new HashMap<>();
        for (Contract c : contracts) {
            List<History> list = new ArrayList<History>();
            for (History h : histories) {
                if (h.getCallerPhoneNumber().equals(c.getPhoneNumber()) &&
                        c.getStartDate().getTime() <= h.getStartTime().getTime() &&
                        h.getStartTime().getTime() < c.getEndDate().getTime()) {
                    list.add(h);
                }
            }
            map.put(c.getKey(), list);
        }
        // すべての履歴データが一致するマスタを持つことを確認
        assertEquals(histories.size(), map.values().stream().mapToInt(s -> s.size()).sum());

        return map;
    }

    /**
     * updateDatabase()のテスト
     */
    @Test
    void testUpdateDatabase() throws Exception {
        HistoryDao historyDao  = testTools.getManager().getHistoryDao();
        List<History> expected = testTools.getHistoryList();

        // 最初のレコードを書き換える
        {
            History history = expected.get(0);
            history.setRecipientPhoneNumber("RECV");
            history.setCharge(999);
            history.setDf(1);
            history.setTimeSecs(221);
            app.setHistory(history);
            testTools.execute(() -> {
                app.updateDatabase(null, historyDao);
            });
        }

        // 12番目のレコードを書き換える
        {
            History history = expected.get(12);
            history.setRecipientPhoneNumber("TEST_NUMBER");
            history.setCharge(55899988);
            history.setDf(0);
            history.setTimeSecs(22551);
            app.setHistory(history);
            testTools.execute(() -> {
                app.updateDatabase(null, historyDao);
            });
        }

        // アプリによる更新後の値が期待した値であることの確認
        List<History> actual = testTools.getHistoryList();
        assertEquals(expected.size(), actual.size());
        for(int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), actual.get(i), " i = " + i);
        }
    }
}
