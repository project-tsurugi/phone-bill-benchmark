package com.tsurugidb.benchmark.phonebill.app;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class TsurugidbWatcherTest {

    @Test
    void testParseMemoryValue() {
        // テスト用の入力文字列と期待される出力値を設定する
        String input1 = "VmSize:       1234 kB";
        long expected1 = 1234L * 1024;
        String input2 = "VmPeak:       5678";
        long expected2 = 5678L;
        String input3 = "VmRSS:        9012 mB";
        long expected3 = 9012L * 1024 * 1024;

        // テストを実行する
        assertEquals(expected1, TsurugidbWatcher.parseMemoryValue(input1));
        assertEquals(expected2, TsurugidbWatcher.parseMemoryValue(input2));
        assertEquals(expected3, TsurugidbWatcher.parseMemoryValue(input3));

        // 不正な入力の場合には-1が返ることを確認する
        assertEquals(-1, TsurugidbWatcher.parseMemoryValue(""));
        assertEquals(-1, TsurugidbWatcher.parseMemoryValue("VmSize:"));
        assertEquals(-1, TsurugidbWatcher.parseMemoryValue("VmSize: abc"));
    }

}
