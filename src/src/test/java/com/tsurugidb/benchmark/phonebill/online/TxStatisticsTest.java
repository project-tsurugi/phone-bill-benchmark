package com.tsurugidb.benchmark.phonebill.online;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.online.TxStatistics.TxStatisticsBundle;

class TxStatisticsTest {

    @Test
    final void test() {
        TxStatistics.clear();
        TxStatistics.addLatencyFotTxLabel(TxLabel.ONLINE_MASTER_DELETE, 52 * 1000 * 1000); // 52ミリ秒
        TxStatistics.addLatencyFotTxLabel(TxLabel.ONLINE_MASTER_DELETE, 25 * 1000 * 1000); // 25ミリ秒
        TxStatistics.addLatencyFotTxLabel(TxLabel.ONLINE_MASTER_DELETE, 35 * 1000 * 1000); // 35ミリ秒
        TxStatistics.addLatencyFotTxLabel(TxLabel.ONLINE_MASTER_INSERT, 112 * 1000 * 1000); // 112ミリ秒
        TxStatistics.setDedicatedTimeMills(10000); // 10000ミリ秒 = 10秒

        Map<TxLabel, TxStatistics> map = TxStatistics.map;
        assertEquals(2, map.size());
        TxStatistics statistics;
        statistics= map.get(TxLabel.ONLINE_MASTER_DELETE);
        assertEquals(3, statistics.getCount());
        assertEquals(52, statistics.getMaxLatency());
        assertEquals(25, statistics.getMinLatency());
        assertEquals((52 + 25 +35) / 3d, statistics.getAverageLatency(), 1e-9);

        statistics= map.get(TxLabel.ONLINE_MASTER_INSERT);
        assertEquals(1, statistics.getCount());
        assertEquals(112, statistics.getMaxLatency());
        assertEquals(112, statistics.getMinLatency());
        assertEquals(112, statistics.getAverageLatency(), 1e-9);

        System.out.println(TxStatistics.getReport());
        String expect ="| title | tx option | dedicated time[ms] | numbers of txs | latency<br>avg[ms] | latency<br>min[ms] | latency<br>max[ms] | committed tx through put[task/s] |\n"
                + "|-------|-----------|-------------------:|---------------:|-------------------:|-------------------:|-------------------:|---------------------------------:|\n"
                + "|ONLINE_MASTER_DELETE|OCC3, LTX1|10,000|3|37.333|25.000|52.000|0.300|\n"
                + "|ONLINE_MASTER_INSERT|OCC3, LTX1|10,000|1|112.000|112.000|112.000|0.100|\n";

        assertEquals(expect, TxStatistics.getReport());
        TxStatistics.saveTxStatisticsBundle("base");

        TxStatistics.clear();
        TxStatistics.addLatencyFotTxLabel(TxLabel.ONLINE_MASTER_DELETE, 350 * 1000 * 1000); // 350ミリ秒
        TxStatistics.addLatencyFotTxLabel(TxLabel.ONLINE_MASTER_INSERT, 240 * 1000 * 1000); // 240ミリ秒
        TxStatistics.setDedicatedTimeMills(20000); // 10000ミリ秒 = 20秒

        map = TxStatistics.map;
        assertEquals(2, map.size());
        statistics= map.get(TxLabel.ONLINE_MASTER_DELETE);
        assertEquals(1, statistics.getCount(), 1e-9);
        assertEquals(350, statistics.getMaxLatency());
        assertEquals(350, statistics.getMinLatency());
        assertEquals(350, statistics.getAverageLatency());

        statistics= map.get(TxLabel.ONLINE_MASTER_INSERT);
        assertEquals(1, statistics.getCount());
        assertEquals(240, statistics.getMaxLatency());
        assertEquals(240, statistics.getMinLatency());
        assertEquals(240, statistics.getAverageLatency());

        TxStatisticsBundle baseline = TxStatistics.getStatisticsBundle("base");
        System.out.println(TxStatistics.getReport(baseline));
        expect ="| title | tx option | dedicated time[ms] | numbers of txs | latency<br>avg[ms] | latency<br>min[ms] | latency<br>max[ms] | committed tx through put[task/s] |\n"
                + "|-------|-----------|-------------------:|---------------:|-------------------:|-------------------:|-------------------:|---------------------------------:|\n"
                + "|ONLINE_MASTER_DELETE|OCC3, LTX1|20,000|1|350.000<br>(937.50%)|350.000<br>(1,400.00%)|350.000<br>(673.08%)|0.050<br>(16.67%)|\n"
                + "|ONLINE_MASTER_INSERT|OCC3, LTX1|20,000|1|240.000<br>(214.29%)|240.000<br>(214.29%)|240.000<br>(214.29%)|0.050<br>(50.00%)|\n";

        assertEquals(expect, TxStatistics.getReport(baseline));

    }

}
