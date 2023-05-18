package com.tsurugidb.benchmark.phonebill.online;

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
     * 実行時間(単位:ミリ秒)
     */
    private static long dedicatedTimeMills = 0;

    public static void setDedicatedTimeMills(long dedicatedTimeMills) {
        TxStatistics.dedicatedTimeMills = dedicatedTimeMills;
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
     * レポートを作成する
     *
     * @param dedicatedTimeMills
     * @return
     */
    public static String getReport() {
        StringBuilder sb = new StringBuilder();
        // ヘッダ
        sb.append("| title | tx option | dedicated time[ms] | numbers of txs | latency<br>avg[ms] | latency<br>min[ms] | latency<br>max[ms] | committed tx through put[task/s] |");
        sb.append("\n");
        sb.append("|-------|-----------|-------------------:|---------------:|-------------------:|-------------------:|-------------------:|---------------------------------:|");
        sb.append("\n");
        // 各TxLabelごとの統計情報
        map.keySet().stream().sorted().map(k -> map.get(k)).forEach(s -> {
            sb.append(s.toString());
            sb.append("\n");
        });
        return sb.toString();
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

    long getMaxLatency() {
        return maxLatency.get();
    }

    long getMinLatency() {
        return minLatency.get();
    }

    double getAverageLatency() {
        long totalCount = count.get();
        if (totalCount > 0) {
            return (double) totalLatency.get() / totalCount;
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

    @Override
    public String toString() {
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
            sb.append(String.format("%,.3f", getAverageLatency() / 1000d / 1000d));
            sb.append("|");
            sb.append(String.format("%,.3f", getMinLatency() / 1000d / 1000d));
            sb.append("|");
            sb.append(String.format("%,.3f", getMaxLatency() / 1000d / 1000d));
            sb.append("|");

            // committed tx through put
            sb.append(String.format("%,.3f", ((double)getCount()) / dedicatedTimeMills * 1000d));
            sb.append("|");
        }

        return sb.toString();
    }}
