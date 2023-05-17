package com.tsurugidb.benchmark.phonebill.online;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.online.RandomKeySelector.KeyPosition;
import com.tsurugidb.benchmark.phonebill.util.TestRandom;

class RandomKeySelectorTest {
	private TestRandom random = new TestRandom();


	@Test
	final void testRandomKeySelector() {
		// keysが空のケース
		RandomKeySelector<Integer> ks;

		ks = new RandomKeySelector<Integer>(Collections.emptySet(), random);
		checkEmpty(ks);

		// keysがnullの要素を含むケース
		IllegalArgumentException ie;
		ie = assertThrows(IllegalArgumentException.class, () -> {
			new RandomKeySelector<Integer>(Arrays.asList(1, 2, 3, null), random);
		});
		assertEquals(RandomKeySelector.ERROR_NULL_ELEMENTS, ie.getMessage());

		// keysが重複する要素を含むケース
		ie = assertThrows(IllegalArgumentException.class, () -> {
			new RandomKeySelector<Integer>(Arrays.asList(1, 2, 3, 2), random);
		});
		assertEquals(RandomKeySelector.ERROR_DUPLICATE_ELEMENTS + "2.", ie.getMessage());

		// keysが要素を一つだけ持つケース

		ks= new RandomKeySelector<Integer>(Arrays.asList(5), random);
		assertIterableEquals(ks.keyList, Arrays.asList(5));
		assertIterableEquals(ks.aloKeyList, Arrays.asList(5));
		assertEquals(1, ks.keyPositionMap.size());
		assertEquals(toSet(5), ks.keyPositionMap.keySet());
		assertEquals(new KeyPosition(0, 0), ks.keyPositionMap.get(5));


		// keysが複数の要素を持つケース1
		random.setValues(0, 0, 0, 0);
		ks = new RandomKeySelector<Integer>(Arrays.asList(1, 2, 3, 4, 5), random);
		assertIterableEquals(ks.keyList, Arrays.asList(1, 2, 3, 4, 5));
		assertIterableEquals(ks.aloKeyList, Arrays.asList(2, 3, 4, 5, 1));
		assertEquals(5, ks.keyPositionMap.size());
		assertEquals(toSet(1,2,3,4,5), ks.keyPositionMap.keySet());
		assertEquals(new KeyPosition(0, 4), ks.keyPositionMap.get(1));
		assertEquals(new KeyPosition(1, 0), ks.keyPositionMap.get(2));
		assertEquals(new KeyPosition(2, 1), ks.keyPositionMap.get(3));
		assertEquals(new KeyPosition(3, 2), ks.keyPositionMap.get(4));
		assertEquals(new KeyPosition(4, 3), ks.keyPositionMap.get(5));

		// keysが複数の要素を持つケース2
		random.setValues(3, 1, 1, 0);
		ks = new RandomKeySelector<Integer>(Arrays.asList(13, 31, 24, 51, 18), random);
		assertIterableEquals(ks.keyList, Arrays.asList(13, 31, 24, 51, 18));
		assertIterableEquals(ks.aloKeyList, Arrays.asList(24, 13, 18, 31, 51));
		assertEquals(5, ks.keyPositionMap.size());
		assertEquals(toSet(13,31,24,51,18), ks.keyPositionMap.keySet());
		assertEquals(new KeyPosition(0, 1), ks.keyPositionMap.get(13));
		assertEquals(new KeyPosition(1, 3), ks.keyPositionMap.get(31));
		assertEquals(new KeyPosition(2, 0), ks.keyPositionMap.get(24));
		assertEquals(new KeyPosition(3, 4), ks.keyPositionMap.get(51));
		assertEquals(new KeyPosition(4, 2), ks.keyPositionMap.get(18));

	}

	private Set<Integer> toSet(Integer ...args) {
		return new HashSet<Integer>(Arrays.asList(args));
	}


	@Test
	final void testRemove() {
		// テストデータ作成
		random.setValues(0, 0, 0, 0);
		RandomKeySelector<Integer> ks = new RandomKeySelector<Integer>(Arrays.asList(1, 2, 3, 4, 5), random);
		assertIterableEquals(Arrays.asList(1, 2, 3, 4, 5), ks.keyList);
		assertIterableEquals(Arrays.asList(2, 3, 4, 5, 1), ks.aloKeyList);
		assertEquals(5, ks.keyPositionMap.size());
		assertEquals(toSet(1,2,3,4,5), ks.keyPositionMap.keySet());
		assertEquals(new KeyPosition(0, 4), ks.keyPositionMap.get(1));
		assertEquals(new KeyPosition(1, 0), ks.keyPositionMap.get(2));
		assertEquals(new KeyPosition(2, 1), ks.keyPositionMap.get(3));
		assertEquals(new KeyPosition(3, 2), ks.keyPositionMap.get(4));
		assertEquals(new KeyPosition(4, 3), ks.keyPositionMap.get(5));

		// 存在しないエントリを削除
		assertFalse(ks.remove(6));
		assertIterableEquals(Arrays.asList(1, 2, 3, 4, 5), ks.keyList);
		assertIterableEquals(Arrays.asList(2, 3, 4, 5, 1), ks.aloKeyList);
		assertEquals(5, ks.keyPositionMap.size());
		assertEquals(toSet(1,2,3,4,5), ks.keyPositionMap.keySet());
		assertEquals(new KeyPosition(0, 4), ks.keyPositionMap.get(1));
		assertEquals(new KeyPosition(1, 0), ks.keyPositionMap.get(2));
		assertEquals(new KeyPosition(2, 1), ks.keyPositionMap.get(3));
		assertEquals(new KeyPosition(3, 2), ks.keyPositionMap.get(4));
		assertEquals(new KeyPosition(4, 3), ks.keyPositionMap.get(5));

		// aloKeyListの最後のキーを削除
		assertTrue(ks.remove(5));
		assertIterableEquals(Arrays.asList(1, 2, 3, 4), ks.keyList);
		assertIterableEquals(Arrays.asList(2, 3, 4, 1), ks.aloKeyList);
		assertEquals(4, ks.keyPositionMap.size());
		assertEquals(toSet(1,2,3, 4), ks.keyPositionMap.keySet());
		assertEquals(new KeyPosition(0, 3), ks.keyPositionMap.get(1));
		assertEquals(new KeyPosition(1, 0), ks.keyPositionMap.get(2));
		assertEquals(new KeyPosition(2, 1), ks.keyPositionMap.get(3));
		assertEquals(new KeyPosition(3, 2), ks.keyPositionMap.get(4));

		// keyListの最後のキーを削除、aloKeyListの最初の要素を削除
		assertTrue(ks.remove(1));
		assertIterableEquals(Arrays.asList(4, 2, 3), ks.keyList);
		assertIterableEquals(Arrays.asList(2, 3, 4), ks.aloKeyList);
		assertEquals(3, ks.keyPositionMap.size());
		assertEquals(toSet(4,2,3), ks.keyPositionMap.keySet());
		assertEquals(new KeyPosition(1, 0), ks.keyPositionMap.get(2));
		assertEquals(new KeyPosition(2, 1), ks.keyPositionMap.get(3));
		assertEquals(new KeyPosition(0, 2), ks.keyPositionMap.get(4));

		// keyListの再訴のキーを削除
		assertTrue(ks.remove(4));
		assertIterableEquals( Arrays.asList(3, 2), ks.keyList);
		assertIterableEquals(Arrays.asList(2, 3), ks.aloKeyList);
		assertEquals(2, ks.keyPositionMap.size());
		assertEquals(toSet(2,3), ks.keyPositionMap.keySet());
		assertEquals(new KeyPosition(1, 0), ks.keyPositionMap.get(2));
		assertEquals(new KeyPosition(0, 1), ks.keyPositionMap.get(3));

		// エントリがなくなるまで削除
		assertTrue(ks.remove(2));
		int t = 3;
		checkSingleEntry(ks, t);

		assertTrue(ks.remove(3));
		checkEmpty(ks);

		// エントリが空の状態で削除
		assertFalse(ks.remove(3));
		checkEmpty(ks);
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
		assertEquals(new KeyPosition(0, 0), ks.keyPositionMap.get(key));
	}

	/**
	 * ksが空かどうかを調べる
	 *
	 * @param ks
	 */
	void checkEmpty(RandomKeySelector<Integer> ks) {
		assertIterableEquals(Collections.emptyList(),ks.keyList );
		assertIterableEquals(Collections.emptyList(),ks.aloKeyList );
		assertEquals(0, ks.keyPositionMap.size());
		assertEquals(Collections.emptySet(), ks.keyPositionMap.keySet());
	}

	@Test
	final void testAdd() {
		// 空の
	}

}
