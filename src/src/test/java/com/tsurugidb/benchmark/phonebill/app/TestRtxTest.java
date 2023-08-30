package com.tsurugidb.benchmark.phonebill.app;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.TestRtx.Statistics;

class TestRtxTest {

    @Test
    final void testStatistics() {
        String expect;

        // listがからのとき
        Statistics.clear();
        expect = "No data";
        assertEquals(expect, Statistics.createReport());

        // listの要素が一つ
        Statistics.clear();
        Statistics.add(1000*1000L);
        expect =  "最大値 = 1,000,000\n"
                + "最小値 = 1,000,000\n"
                + "平均値 = 1,000,000\n"
                + "度数分布\n"
                + "524,288以上1,048,576未満: 1\n";
        assertEquals(expect, Statistics.createReport());


        // listの要素が複数
        Statistics.clear();
        Statistics.add(1L);
        Statistics.add(2L);
        Statistics.add(5L);
        Statistics.add(9L);
        Statistics.add(7L);
        Statistics.add(3L);
        Statistics.add(1L);
        expect =  "最大値 = 9\n"
                + "最小値 = 1\n"
                + "平均値 = 4\n"
                + "度数分布\n"
                + "1以上2未満: 2\n"
                + "2以上4未満: 2\n"
                + "4以上8未満: 2\n"
                + "8以上16未満: 1\n";
        assertEquals(expect, Statistics.createReport());

    }

}
