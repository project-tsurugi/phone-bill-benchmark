package com.tsurugidb.benchmark.phonebill.online;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.online.RandomKeySelector.KeyPositions;
import com.tsurugidb.benchmark.phonebill.util.TestRandom;

class RandomKeySelectorTest {
    private TestRandom random = new TestRandom();

    @Test
    final void testRandomKeySelector() {
        // keysが空のケース
        RandomKeySelector<Integer> ks;

        ks = new RandomKeySelector<Integer>(Collections.emptySet(), random, 0d);
        checkEmpty(ks);

        // keysがnullの要素を含むケース
        IllegalArgumentException ie;
        ie = assertThrows(IllegalArgumentException.class, () -> {
            new RandomKeySelector<Integer>(Arrays.asList(1, 2, 3, null), random, 0d);
        });
        assertEquals(RandomKeySelector.ERROR_NULL_ELEMENTS, ie.getMessage());

        // keysが重複する要素を含むケース
        ie = assertThrows(IllegalArgumentException.class, () -> {
            new RandomKeySelector<Integer>(Arrays.asList(1, 2, 3, 2), random, 0d);
        });
        assertEquals(RandomKeySelector.ERROR_DUPLICATE_ELEMENTS + "2.", ie.getMessage());

        // keysが要素を一つだけ持つケース
        ks = new RandomKeySelector<Integer>(Arrays.asList(5), random, 0d);
        assertIterableEquals(ks.keyList, Arrays.asList(5));
        assertIterableEquals(ks.aloKeyList, Arrays.asList(5));
        assertEquals(1, ks.keyPositionMap.size());
        assertEquals(toSet(5), ks.keyPositionMap.keySet());
        assertKeyPostions(0, 0, ks.keyPositionMap.get(5));

        // keysが複数の要素を持つケース1
        random.setValues(0, 0, 0, 0);
        ks = new RandomKeySelector<Integer>(Arrays.asList(1, 2, 3, 4, 5), random, 0d);
        assertIterableEquals(ks.keyList, Arrays.asList(1, 2, 3, 4, 5));
        assertIterableEquals(ks.aloKeyList, Arrays.asList(2, 3, 4, 5, 1));
        assertEquals(5, ks.keyPositionMap.size());
        assertEquals(toSet(1, 2, 3, 4, 5), ks.keyPositionMap.keySet());
        assertKeyPostions(0, 4, ks.keyPositionMap.get(1));
        assertKeyPostions(1, 0, ks.keyPositionMap.get(2));
        assertKeyPostions(2, 1, ks.keyPositionMap.get(3));
        assertKeyPostions(3, 2, ks.keyPositionMap.get(4));
        assertKeyPostions(4, 3, ks.keyPositionMap.get(5));

        // keysが複数の要素を持つケース2
        random.setValues(3, 1, 1, 0);
        ks = new RandomKeySelector<Integer>(Arrays.asList(13, 31, 24, 51, 18), random, 0d);
        assertIterableEquals(ks.keyList, Arrays.asList(13, 31, 24, 51, 18));
        assertIterableEquals(ks.aloKeyList, Arrays.asList(24, 13, 18, 31, 51));
        assertEquals(5, ks.keyPositionMap.size());
        assertEquals(toSet(13, 31, 24, 51, 18), ks.keyPositionMap.keySet());
        assertKeyPostions(0, 1, ks.keyPositionMap.get(13));
        assertKeyPostions(1, 3, ks.keyPositionMap.get(31));
        assertKeyPostions(2, 0, ks.keyPositionMap.get(24));
        assertKeyPostions(3, 4, ks.keyPositionMap.get(51));
        assertKeyPostions(4, 2, ks.keyPositionMap.get(18));

        // atLeastOnceSelectRateの範囲チェックのテスト
        ie = assertThrows(IllegalArgumentException.class, () -> {
            new RandomKeySelector<Integer>(Collections.emptyList(), random, -1E-99d);
        });
        assertEquals(RandomKeySelector.ERROR_RANGE, ie.getMessage());

        ie = assertThrows(IllegalArgumentException.class, () -> {
            new RandomKeySelector<Integer>(Collections.emptyList(), random, 1.0000000000001d);
        });
        assertEquals(RandomKeySelector.ERROR_RANGE, ie.getMessage());

        assertDoesNotThrow(() -> {
            new RandomKeySelector<Integer>(Collections.emptyList(), random, 0d);
        });

        assertDoesNotThrow(() -> {
            new RandomKeySelector<Integer>(Collections.emptyList(), random, 1d);
        });

        // coverRateの指定
        ks = new RandomKeySelector<Integer>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), new Random(), 0d, 1d);
        assertEquals(10, ks.keyList.size());
        assertEquals(10, ks.aloKeyList.size());

        ks = new RandomKeySelector<Integer>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), new Random(), 0d, 0.5d);
        assertEquals(5, ks.keyList.size());
        assertEquals(5, ks.aloKeyList.size());

        ks = new RandomKeySelector<Integer>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), new Random(), 0d, 0d);
        assertEquals(0, ks.keyList.size());
        assertEquals(0, ks.aloKeyList.size());


        // coverRateの範囲チェックのテスト
        ie = assertThrows(IllegalArgumentException.class, () -> {
            new RandomKeySelector<Integer>(Collections.emptyList(), random, 1d, -1E-99d);
        });
        assertEquals(RandomKeySelector.ERROR_RANGE, ie.getMessage());

        ie = assertThrows(IllegalArgumentException.class, () -> {
            new RandomKeySelector<Integer>(Collections.emptyList(), random, 1d, 1.0000000000001d);
        });
        assertEquals(RandomKeySelector.ERROR_RANGE, ie.getMessage());

        assertDoesNotThrow(() -> {
            new RandomKeySelector<Integer>(Collections.emptyList(), random, 1d, 0d);
        });

        assertDoesNotThrow(() -> {
            new RandomKeySelector<Integer>(Collections.emptyList(), random, 1d, 1d);
        });
    }

    private Set<Integer> toSet(Integer... args) {
        return new HashSet<Integer>(Arrays.asList(args));
    }

    @Test
    final void testRemove() {
        // テストデータ作成
        RandomKeySelector<Integer> ks;
        ks = createTestData(0d);

        // 存在しないエントリを削除
        assertFalse(ks.remove(6));
        assertIterableEquals(Arrays.asList(1, 2, 3, 4, 5), ks.keyList);
        assertIterableEquals(Arrays.asList(2, 3, 4, 5, 1), ks.aloKeyList);
        assertEquals(5, ks.keyPositionMap.size());
        assertEquals(toSet(1, 2, 3, 4, 5), ks.keyPositionMap.keySet());
        assertKeyPostions(0, 4, ks.keyPositionMap.get(1));
        assertKeyPostions(1, 0, ks.keyPositionMap.get(2));
        assertKeyPostions(2, 1, ks.keyPositionMap.get(3));
        assertKeyPostions(3, 2, ks.keyPositionMap.get(4));
        assertKeyPostions(4, 3, ks.keyPositionMap.get(5));

        // aloKeyListの最後のキーを削除
        assertTrue(ks.remove(5));
        assertIterableEquals(Arrays.asList(1, 2, 3, 4), ks.keyList);
        assertIterableEquals(Arrays.asList(2, 3, 4, 1), ks.aloKeyList);
        assertEquals(4, ks.keyPositionMap.size());
        assertEquals(toSet(1, 2, 3, 4), ks.keyPositionMap.keySet());
        assertKeyPostions(0, 3, ks.keyPositionMap.get(1));
        assertKeyPostions(1, 0, ks.keyPositionMap.get(2));
        assertKeyPostions(2, 1, ks.keyPositionMap.get(3));
        assertKeyPostions(3, 2, ks.keyPositionMap.get(4));

        // keyListの最後のキーを削除、aloKeyListの最初の要素を削除
        assertTrue(ks.remove(1));
        assertIterableEquals(Arrays.asList(4, 2, 3), ks.keyList);
        assertIterableEquals(Arrays.asList(2, 3, 4), ks.aloKeyList);
        assertEquals(3, ks.keyPositionMap.size());
        assertEquals(toSet(4, 2, 3), ks.keyPositionMap.keySet());
        assertKeyPostions(1, 0, ks.keyPositionMap.get(2));
        assertKeyPostions(2, 1, ks.keyPositionMap.get(3));
        assertKeyPostions(0, 2, ks.keyPositionMap.get(4));

        // keyListの再訴のキーを削除
        assertTrue(ks.remove(4));
        assertIterableEquals(Arrays.asList(3, 2), ks.keyList);
        assertIterableEquals(Arrays.asList(2, 3), ks.aloKeyList);
        assertEquals(2, ks.keyPositionMap.size());
        assertEquals(toSet(2, 3), ks.keyPositionMap.keySet());
        assertKeyPostions(1, 0, ks.keyPositionMap.get(2));
        assertKeyPostions(0, 1, ks.keyPositionMap.get(3));

        // エントリがなくなるまで削除
        assertTrue(ks.remove(2));
        int t = 3;
        checkSingleEntry(ks, t);

        assertTrue(ks.remove(3));
        checkEmpty(ks);

        // エントリが空の状態で削除
        assertFalse(ks.remove(3));
        checkEmpty(ks);

        // 一部のエントリが、aloKeyListに存在しないケース
        random.setValues(0, 0);
        ks = new RandomKeySelector<Integer>(Arrays.asList(1, 2), random, 0d);
        ks.add(3);
        ks.add(4);
        assertIterableEquals(Arrays.asList(1, 2, 3, 4), ks.keyList);
        assertIterableEquals(Arrays.asList(2, 1), ks.aloKeyList);
        assertEquals(4, ks.keyPositionMap.size());
        assertEquals(toSet(1, 2, 3, 4), ks.keyPositionMap.keySet());
        assertKeyPostions(0, 1, ks.keyPositionMap.get(1));
        assertKeyPostions(1, 0, ks.keyPositionMap.get(2));
        assertKeyPostions(2, -1, ks.keyPositionMap.get(3));
        assertKeyPostions(3, -1, ks.keyPositionMap.get(4));

        ks.remove(3);
        assertIterableEquals(Arrays.asList(1, 2, 4), ks.keyList);
        assertIterableEquals(Arrays.asList(2, 1), ks.aloKeyList);
        assertEquals(3, ks.keyPositionMap.size());
        assertEquals(toSet(1, 2, 4), ks.keyPositionMap.keySet());
        assertKeyPostions(0, 1, ks.keyPositionMap.get(1));
        assertKeyPostions(1, 0, ks.keyPositionMap.get(2));
        assertKeyPostions(2, -1, ks.keyPositionMap.get(4));

        ks.remove(1);
        assertIterableEquals(Arrays.asList(4, 2), ks.keyList);
        assertIterableEquals(Arrays.asList(2), ks.aloKeyList);
        assertEquals(2, ks.keyPositionMap.size());
        assertEquals(toSet(2, 4), ks.keyPositionMap.keySet());
        assertKeyPostions(1, 0, ks.keyPositionMap.get(2));
        assertKeyPostions(0, -1, ks.keyPositionMap.get(4));

        ks.remove(2);
        assertIterableEquals(Arrays.asList(4), ks.keyList);
        assertIterableEquals(Collections.emptyList(), ks.aloKeyList);
        assertEquals(1, ks.keyPositionMap.size());
        assertEquals(toSet(4), ks.keyPositionMap.keySet());
        assertKeyPostions(0, -1, ks.keyPositionMap.get(4));

        ks.remove(6); // 存在しないエントリを削除
        assertIterableEquals(Arrays.asList(4), ks.keyList);
        assertIterableEquals(Collections.emptyList(), ks.aloKeyList);
        assertEquals(1, ks.keyPositionMap.size());
        assertEquals(toSet(4), ks.keyPositionMap.keySet());
        assertKeyPostions(0, -1, ks.keyPositionMap.get(4));

        ks.remove(4); // 存在しないエントリを削除
        checkEmpty(ks);

    }

    RandomKeySelector<Integer> createTestData(double aloSelectRate) {
        RandomKeySelector<Integer> ks;
        random.setValues(0, 0, 0, 0, 4, 1, 3, 2, 0, 3);
        ks = new RandomKeySelector<Integer>(Arrays.asList(1, 2, 3, 4, 5), random, aloSelectRate);
        assertIterableEquals(Arrays.asList(1, 2, 3, 4, 5), ks.keyList);
        assertIterableEquals(Arrays.asList(2, 3, 4, 5, 1), ks.aloKeyList);
        assertEquals(5, ks.keyPositionMap.size());
        assertEquals(toSet(1, 2, 3, 4, 5), ks.keyPositionMap.keySet());
        assertKeyPostions(0, 4, ks.keyPositionMap.get(1));
        assertKeyPostions(1, 0, ks.keyPositionMap.get(2));
        assertKeyPostions(2, 1, ks.keyPositionMap.get(3));
        assertKeyPostions(3, 2, ks.keyPositionMap.get(4));
        assertKeyPostions(4, 3, ks.keyPositionMap.get(5));
        return ks;
    }

    @Test
    void testGet() {
        // テストデータ作成 (aloKeyListを使わないパターン)
        RandomKeySelector<Integer> ks;
        ks = createTestData(0d);
        assertEquals(5, ks.get());
        assertEquals(2, ks.get());
        assertEquals(4, ks.get());
        assertEquals(3, ks.get());
        assertEquals(1, ks.get());
        assertEquals(4, ks.get());

        // テストデータ作成 (aloKeyListのみを使うパターン)
        ks = createTestData(1d);
        assertEquals(1, ks.get());
        assertIterableEquals(Arrays.asList(2, 3, 4, 5), ks.aloKeyList);
        assertEquals(5, ks.get());
        assertIterableEquals(Arrays.asList(2, 3, 4), ks.aloKeyList);
        assertEquals(4, ks.get());
        assertIterableEquals(Arrays.asList(2, 3), ks.aloKeyList);
        assertEquals(3, ks.get());
        assertIterableEquals(Arrays.asList(2), ks.aloKeyList);
        assertEquals(2, ks.get());
        assertIterableEquals(Arrays.asList(1, 2, 4, 3, 5), ks.aloKeyList);
    }

    @Test
    void testGetAndRemove() {
        // テストデータ作成 (aloKeyListを使わないパターン)
        RandomKeySelector<Integer> ks;
        ks = createTestData(0d);
        assertEquals(5, ks.getAndRemove());
        assertIterableEquals(Arrays.asList(1, 2, 3, 4), ks.keyList);
        assertIterableEquals(Arrays.asList(2, 3, 4, 1), ks.aloKeyList);
        assertEquals(4, ks.keyPositionMap.size());
        assertEquals(toSet(1, 2, 3, 4), ks.keyPositionMap.keySet());
        assertKeyPostions(0, 3, ks.keyPositionMap.get(1));
        assertKeyPostions(1, 0, ks.keyPositionMap.get(2));
        assertKeyPostions(2, 1, ks.keyPositionMap.get(3));
        assertKeyPostions(3, 2, ks.keyPositionMap.get(4));


        // テストデータ作成 (aloKeyListのみを使うパターン)
        ks = createTestData(1d);
        assertEquals(1, ks.getAndRemove());
        assertIterableEquals(Arrays.asList(2, 3, 4, 5), ks.aloKeyList);
        assertIterableEquals(Arrays.asList(5, 2, 3, 4), ks.keyList);
        assertIterableEquals(Arrays.asList(2, 3, 4, 5), ks.aloKeyList);
        assertEquals(4, ks.keyPositionMap.size());
        assertEquals(toSet(2, 3, 4, 5), ks.keyPositionMap.keySet());
        assertKeyPostions(1, 0, ks.keyPositionMap.get(2));
        assertKeyPostions(2, 1, ks.keyPositionMap.get(3));
        assertKeyPostions(3, 2, ks.keyPositionMap.get(4));
        assertKeyPostions(0, 3, ks.keyPositionMap.get(5));

    }


    /**
     * ksが唯一のエントリkeyを持つことを調べる
     *
     * @param ks
     * @param key
     */
    void checkSingleEntry(RandomKeySelector<Integer> ks, int key) {
        assertIterableEquals(Collections.singletonList(key), ks.keyList);
        assertIterableEquals(Collections.singleton(key), ks.aloKeyList);
        assertEquals(1, ks.keyPositionMap.size());
        assertEquals(toSet(key), ks.keyPositionMap.keySet());
        assertKeyPostions(0, 0, ks.keyPositionMap.get(key));
    }

    /**
     * ksが空かどうかを調べる
     *
     * @param ks
     */
    void checkEmpty(RandomKeySelector<Integer> ks) {
        assertIterableEquals(Collections.emptyList(), ks.keyList);
        assertIterableEquals(Collections.emptyList(), ks.aloKeyList);
        assertEquals(0, ks.keyPositionMap.size());
        assertEquals(Collections.emptySet(), ks.keyPositionMap.keySet());
    }

    @Test
    final void testAdd() {
        RandomKeySelector<Integer> ks;

        // 空のRandomKeySelectorにキーを追加していく
        ks = new RandomKeySelector<Integer>(Collections.emptyList(), random, 0d);

        ks.add(3);
        assertIterableEquals(Collections.singletonList(3), ks.keyList);
        assertIterableEquals(Collections.emptyList(), ks.aloKeyList);
        assertEquals(1, ks.keyPositionMap.size());
        assertEquals(toSet(3), ks.keyPositionMap.keySet());
        assertKeyPostions(0, -1, ks.keyPositionMap.get(3));

        ks.add(33);
        assertIterableEquals(Arrays.asList(3, 33), ks.keyList);
        assertIterableEquals(Collections.emptyList(), ks.aloKeyList);
        assertEquals(2, ks.keyPositionMap.size());
        assertEquals(toSet(3, 33), ks.keyPositionMap.keySet());
        assertKeyPostions(0, -1, ks.keyPositionMap.get(3));
        assertKeyPostions(1, -1, ks.keyPositionMap.get(33));

        ks.add(333);
        assertIterableEquals(Arrays.asList(3, 33, 333), ks.keyList);
        assertIterableEquals(Collections.emptyList(), ks.aloKeyList);
        assertEquals(3, ks.keyPositionMap.size());
        assertEquals(toSet(3, 33, 333), ks.keyPositionMap.keySet());
        assertKeyPostions(0, -1, ks.keyPositionMap.get(3));
        assertKeyPostions(1, -1, ks.keyPositionMap.get(33));
        assertKeyPostions(2, -1, ks.keyPositionMap.get(333));

        ks = createTestData(0d);

        ks.add(101);
        assertIterableEquals(Arrays.asList(1, 2, 3, 4, 5, 101), ks.keyList);
        assertIterableEquals(Arrays.asList(2, 3, 4, 5, 1), ks.aloKeyList);
        assertEquals(6, ks.keyPositionMap.size());
        assertEquals(toSet(1, 2, 3, 4, 5, 101), ks.keyPositionMap.keySet());
        assertKeyPostions(0, 4, ks.keyPositionMap.get(1));
        assertKeyPostions(1, 0, ks.keyPositionMap.get(2));
        assertKeyPostions(2, 1, ks.keyPositionMap.get(3));
        assertKeyPostions(3, 2, ks.keyPositionMap.get(4));
        assertKeyPostions(4, 3, ks.keyPositionMap.get(5));
        assertKeyPostions(5, -1, ks.keyPositionMap.get(101));
    }

    void assertKeyPostions(int expectedKeyPostion, int expectedAloKeyPostion, KeyPositions actualKeyPositions) {
        assertEquals(expectedKeyPostion, actualKeyPositions.keyPostion);
        assertEquals(expectedAloKeyPostion, actualKeyPositions.aloKeyPostion);
    }

}
