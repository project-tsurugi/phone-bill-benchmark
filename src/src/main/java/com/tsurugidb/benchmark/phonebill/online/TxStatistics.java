package com.tsurugidb.benchmark.phonebill.online;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.tsurugidb.benchmark.phonebill.db.TxLabel;

/**
 * トランザクションに関する統計情報を管理するクラス
 */
public class TxStatistics {
    /**
     * TxLabelと統計情報を紐付けるmap
     */
    static Map<TxLabel, TxStatistics> map = new ConcurrentHashMap<TxLabel, TxStatistics>();


    /**
     * TxStatisticsBundleに名前をつけて保存するためのMap
     */
    static Map<String, TxStatisticsBundle> bundleMap = new HashMap<>();


    /**
     * 実行時間(単位:ミリ秒)
     */
    private static long dedicatedTimeMills = 0;

    public static void setDedicatedTimeMills(long dedicatedTimeMills) {
        TxStatistics.dedicatedTimeMills = dedicatedTimeMills;
        map.values().forEach(v -> {
            v.thisDedicatedTimeMills = dedicatedTimeMills;
        });
    }

    /**
     * 指定のラベルのTXの遅延を登録する
     *
     * @param lavbel TXを表すラベル
     * @param nanos  遅延(単位はナノ秒)
     */
    public static void addLatencyFotTxLabel(TxLabel lavbel, long latencyNanos) {
        TxStatistics statistics = map.computeIfAbsent(lavbel, k -> new TxStatistics(lavbel));
        statistics.addLatency(latencyNanos);
    }

    /**
     * 統計情報をクリアする
     */
    public static void clear() {
        map.clear();
    }

    /**
     * StatisticsBundleを返す。この値は、{@link TxStatistics#clear()}の呼び出しで失われない。
     *
     * @return TxStatisticsBundle
     */
    private static TxStatisticsBundle getStatisticsBundle() {
        return new TxStatisticsBundle(map);
    }


    /**
     * TxStatisticsBundleを生成し、名前をつけて保存する。
     *
     * @param name
     */
    public static void saveTxStatisticsBundle(String name) {
        bundleMap.put(name, getStatisticsBundle());
    }


    /**
     * 指定の名前のStatisticsBundleを返す。
     * 本メソッドが返すStatisticsBundleは{@link TxStatistics#getReport()}に使用できる。
     *
     * @param name
     */
    public static TxStatisticsBundle getStatisticsBundle(String name) {
        return bundleMap.get(name);
    }

    /**
     * レポートを作成する。baselineが指定された場合、latencyの項目に、baselineの値とのパーセンテージが追加される。
     *
     * @param baseline 基準となる統計情報
     * @return
     */
    public static String getReport(TxStatisticsBundle baseline) {
        StringBuilder sb = new StringBuilder();
        sb.append("| title | tx option | dedicated time[ms] | numbers of txs | latency<br>avg[ms] | latency<br>min[ms] | latency<br>max[ms] | committed tx through put[task/s] |");
        sb.append("\n");
        sb.append("|-------|-----------|-------------------:|---------------:|-------------------:|-------------------:|-------------------:|---------------------------------:|");
        sb.append("\n");
        // 各TxLabelごとの統計情報
        map.keySet().stream().sorted().forEach(key -> {
            TxStatistics statistics = map.get(key);
            TxStatistics baselineStatistics = baseline == null ? null : baseline.map.get(key);
            sb.append(statistics.toString(baselineStatistics));
            sb.append("\n");
        });
        return sb.toString();
    }


    /**
     * レポートを作成する。
     *
     * @return
     */
    public static String getReport() {
        return getReport(null);
    }



    /**
     * TXのラベル
     */
    private TxLabel label;

    /**
     * 成功したTX数
     */
    private AtomicLong count;
    /**
     * 遅延の最大値(単位はナノ秒)
     */
    private AtomicLong maxLatency;
    /**
     * 遅延の最小値(単位はナノ秒)
     */
    private AtomicLong minLatency;
    /**
     * 遅延の合計値
     */
    private AtomicLong totalLatency;
    /**
     * 実行時間
     * @param label
     */
    private long thisDedicatedTimeMills;

    TxStatistics(TxLabel label) {
        this.label = label;
        count = new AtomicLong(0);
        maxLatency = new AtomicLong(Long.MIN_VALUE);
        minLatency = new AtomicLong(Long.MAX_VALUE);
        totalLatency = new AtomicLong(0);
    }

    long getCount() {
        return count.get();
    }

    double getMaxLatency() {
        return maxLatency.get() / 1000d / 1000d;
    }

    double getMinLatency() {
        return minLatency.get() / 1000d / 1000d;
    }

    double getThroughput() {
        return 1000d * getCount() / thisDedicatedTimeMills;
    }

    double getAverageLatency() {
        long totalCount = count.get();
        if (totalCount > 0) {
            return (double) totalLatency.get() / totalCount / 1000d / 1000d ;
        } else {
            return 0;
        }
    }

    void addLatency(long latency) {
        count.incrementAndGet();
        totalLatency.addAndGet(latency);
        updateMaxLatency(latency);
        updateMinLatency(latency);
    }

    private void updateMaxLatency(long latency) {
        long currentMax = maxLatency.get();
        while (latency > currentMax) {
            if (maxLatency.compareAndSet(currentMax, latency)) {
                break;
            }
            currentMax = maxLatency.get();
        }
    }

    private void updateMinLatency(long latency) {
        long currentMin = minLatency.get();
        while (latency < currentMin) {
            if (minLatency.compareAndSet(currentMin, latency)) {
                break;
            }
            currentMin = minLatency.get();
        }
    }

    /**
     * 統計情報を文字列化する。
     *
     * @param baselineStatistics 基準となる統計情報。
     * @return
     */
    public String toString(TxStatistics baselineStatistics) {
        var sb = new StringBuilder(64);
        sb.append("|");
        sb.append(label);
        sb.append("|");
        sb.append("OCC3, LTX1");
        sb.append("|");

        // dedicated time
        sb.append(String.format("%,d", dedicatedTimeMills));
        sb.append("|");

        {
            // numbers of txs
            sb.append(String.format("%,d", getCount()));
            sb.append("|");

            // latency
            sb.append(formatWithBaseline(getAverageLatency(), baselineStatistics != null ? baselineStatistics.getAverageLatency() : -1d));
            sb.append("|");
            sb.append(formatWithBaseline(getMinLatency(), baselineStatistics != null ? baselineStatistics.getMinLatency() : -1d));
            sb.append("|");
            sb.append(formatWithBaseline(getMaxLatency(), baselineStatistics != null ? baselineStatistics.getMaxLatency() : -1d));
            sb.append("|");
            sb.append(formatWithBaseline(getThroughput(), baselineStatistics != null ? baselineStatistics.getThroughput() : -1d));
            sb.append("|");
        }

        return sb.toString();
    }

    private static String formatWithBaseline(double value, double baseline) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%,.3f", value));
        if (baseline != -1d) {
            sb.append("<br>(");
            sb.append(String.format("%,.2f%%", value / baseline * 100));
            sb.append(")");
        }

        return sb.toString();
    }

    /**
     * 複数の統計情報をまとめて保持するクラス。
     */
    public static class TxStatisticsBundle {
        Map<TxLabel, TxStatistics> map = new ConcurrentHashMap<TxLabel, TxStatistics>();

        public TxStatisticsBundle(Map<TxLabel, TxStatistics> map) {
            this.map = new HashMap<>(map);
        }
    }

}
