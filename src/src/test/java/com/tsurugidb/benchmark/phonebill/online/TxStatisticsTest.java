package com.tsurugidb.benchmark.phonebill.online;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.db.TxLabel;

class TxStatisticsTest {

    @Test
    final void test() {
        TxStatistics.clear();
        String header = TxStatistics.getReport();

        TxStatistics.addLatencyFotTxLabel(TxLabel.ONLINE_MASTER_DELETE, 52);
        TxStatistics.addLatencyFotTxLabel(TxLabel.ONLINE_MASTER_DELETE, 25);
        TxStatistics.addLatencyFotTxLabel(TxLabel.ONLINE_MASTER_DELETE, 35);
        TxStatistics.addLatencyFotTxLabel(TxLabel.ONLINE_MASTER_INSERT, 112);

        Map<TxLabel, TxStatistics> map = TxStatistics.map;
        assertEquals(2, map.size());
        TxStatistics statistics;
        statistics= map.get(TxLabel.ONLINE_MASTER_DELETE);
        assertEquals(3, statistics.getCount());
        assertEquals(52, statistics.getMaxLatency());
        assertEquals(25, statistics.getMinLatency());
        assertEquals((52 + 25 +35) / 3d , statistics.getAverageLatency(), 1e-9);

        statistics= map.get(TxLabel.ONLINE_MASTER_INSERT);
        assertEquals(1, statistics.getCount());
        assertEquals(112, statistics.getMaxLatency());
        assertEquals(112, statistics.getMinLatency());
        assertEquals(112d , statistics.getAverageLatency(), 1e-9);

        System.out.println(TxStatistics.getReport());
    }

}
