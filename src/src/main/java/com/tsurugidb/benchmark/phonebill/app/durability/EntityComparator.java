package com.tsurugidb.benchmark.phonebill.app.durability;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;

public class EntityComparator<T, K> {
    private List<T> expectedOnly;
    private List<T> actualOnly;
    private List<T> sameInBoth;
    private List<Map.Entry<T, T>> differentInBoth;
    private String name;
    private BiPredicate<T, T> customEquality;


    // customEqualityをオプショナルにするためのオーバーロードされたコンストラクタ
    public EntityComparator(Collection<T> expected, Collection<T> actual, Function<T, K> keyExtractor, String name) {
        this(expected, actual, keyExtractor, Object::equals, name);
    }

    public EntityComparator(Collection<T> expected, Collection<T> actual, Function<T, K> keyExtractor, BiPredicate<T, T> customEquality, String name) {
            Map<K, T> expectedMap = new HashMap<>();
        this.expectedOnly = new ArrayList<>();
        this.actualOnly = new ArrayList<>();
        this.sameInBoth = new ArrayList<>();
        this.differentInBoth = new ArrayList<>();
        this.name = name;
        this.customEquality = customEquality;

        // まずexpectedのマップを作成
        for (T entity : expected) {
            K key = keyExtractor.apply(entity);
            expectedMap.put(key, entity);
        }

        // actualを処理
        for (T entity : actual) {
            K key = keyExtractor.apply(entity);
            if (expectedMap.containsKey(key)) {
                T expectedEntity = expectedMap.remove(key);
                if (this.customEquality.test(expectedEntity, entity)) {
                    this.sameInBoth.add(entity);
                } else {
                    this.differentInBoth.add(new AbstractMap.SimpleEntry<>(expectedEntity, entity));
                }
            } else {
                this.actualOnly.add(entity);
            }
        }

        // expectedにのみ存在するエンティティを抽出
        this.expectedOnly.addAll(expectedMap.values());
    }

    public List<T> getExpectedOnly() {
        return expectedOnly;
    }

    public List<T> getActualOnly() {
        return actualOnly;
    }

    public List<T> getSameInBoth() {
        return sameInBoth;
    }

    public List<Map.Entry<T, T>> getDifferentInBoth() {
        return differentInBoth;
    }

    public boolean areCollectionsExactlyEqual() {
        return expectedOnly.isEmpty() && actualOnly.isEmpty() && differentInBoth.isEmpty();
    }

    public String getComparisonResultAsString() {
        StringBuilder sb = new StringBuilder();

        sb.append(name + " present only expected:\n");
        for (T entity : expectedOnly) {
            sb.append(entity.toString()).append("\n");
        }

        sb.append("\n" + name + " present only actual:\n");
        for (T entity : actualOnly) {
            sb.append(entity.toString()).append("\n");
        }

        sb.append("\n" + name + " that have changed:\n");
        for (Map.Entry<T, T> entry : differentInBoth) {
            sb.append("expected: ");
            sb.append(entry.getKey().toString());
            sb.append("\nactual: ");
            sb.append(entry.getValue().toString());
            sb.append("\n\n");
        }

        return sb.toString();
    }
}
