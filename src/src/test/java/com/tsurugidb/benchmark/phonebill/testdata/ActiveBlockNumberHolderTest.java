package com.tsurugidb.benchmark.phonebill.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActiveBlockNumberHolderTest {

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@BeforeEach
	void setUp() throws Exception {
	}

	@Test
	void testSetActiveBlocks() {
		ActiveBlockNumberHolder holder = new ActiveBlockNumberHolder();
		// 生成直後
		assertEquals(Collections.emptyList(), holder.getActiveBlocks());
		assertEquals(0, holder.getNumberOfActiveBlacks());
		assertEquals(-1, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);

		// 空のリスト
		holder.setActiveBlocks(Collections.emptyList());
		assertEquals(Collections.emptyList(), holder.getActiveBlocks());
		assertEquals(0, holder.getNumberOfActiveBlacks());
		assertEquals(-1, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);

		// 要素数が1で値が0
		holder.setActiveBlocks(Arrays.asList(0));
		assertEquals(Collections.emptyList(), holder.getActiveBlocks());
		assertEquals(1, holder.getNumberOfActiveBlacks());
		assertEquals(0, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);

		// 要素数が1で値が0以外
		holder.setActiveBlocks(Arrays.asList(1));
		assertEquals(Arrays.asList(1), holder.getActiveBlocks());
		assertEquals(1, holder.getNumberOfActiveBlacks());
		assertEquals(-1, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);

		// 先頭部の省略あり
		holder.setActiveBlocks(Arrays.asList(5, 0, 1, 2, 8));
		assertEquals(Arrays.asList(5, 8), holder.getActiveBlocks());
		assertEquals(5, holder.getNumberOfActiveBlacks());
		assertEquals(2, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);

		// 先頭部の省略なし
		holder.setActiveBlocks(Arrays.asList(0, 7, 6, 4));
		assertEquals(Arrays.asList(4, 6, 7), holder.getActiveBlocks());
		assertEquals(4, holder.getNumberOfActiveBlacks());
		assertEquals(0, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);

		// 0から始まらないケース
		holder.setActiveBlocks(Arrays.asList(3, 7, 2, 9));
		assertEquals(Arrays.asList(2, 3, 7, 9), holder.getActiveBlocks());
		assertEquals(4, holder.getNumberOfActiveBlacks());
		assertEquals(-1, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);

		// ブロック番号が完全に連続なケース
		holder.setActiveBlocks(Arrays.asList(0, 1, 2, 3));
		assertEquals(Collections.emptyList(), holder.getActiveBlocks());
		assertEquals(4, holder.getNumberOfActiveBlacks());
		assertEquals(3, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);


	}


	@Test
	void testAddActiveBlockNumber() {
		ActiveBlockNumberHolder holder = new ActiveBlockNumberHolder();

		// 空のリストに0から順番に追加
		holder.setActiveBlocks(Collections.emptyList());
		assertEquals(Collections.emptyList(), holder.getActiveBlocks());
		assertEquals(0, holder.getNumberOfActiveBlacks());
		assertEquals(-1, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);

		holder.addActiveBlockNumber(0);
		assertEquals(Collections.emptyList(), holder.getActiveBlocks());
		assertEquals(1, holder.getNumberOfActiveBlacks());
		assertEquals(0, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);

		holder.addActiveBlockNumber(1);
		assertEquals(Collections.emptyList(), holder.getActiveBlocks());
		assertEquals(2, holder.getNumberOfActiveBlacks());
		assertEquals(1, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);

		holder.addActiveBlockNumber(2);
		assertEquals(Collections.emptyList(), holder.getActiveBlocks());
		assertEquals(3, holder.getNumberOfActiveBlacks());
		assertEquals(2, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);

		holder.addActiveBlockNumber(3);
		assertEquals(Collections.emptyList(), holder.getActiveBlocks());
		assertEquals(4, holder.getNumberOfActiveBlacks());
		assertEquals(3, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);


		// 0から始まらず1を含まないリストに0が追加されるケース
		holder.setActiveBlocks(Arrays.asList(3, 7, 2, 9));
		assertEquals(Arrays.asList(2, 3, 7, 9), holder.getActiveBlocks());
		assertEquals(4, holder.getNumberOfActiveBlacks());
		assertEquals(-1, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);

		holder.addActiveBlockNumber(0);
		assertEquals(Arrays.asList(2, 3, 7, 9), holder.getActiveBlocks());
		assertEquals(5, holder.getNumberOfActiveBlacks());
		assertEquals(0, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);

		// 0から始まらず1を含むリストに0が追加されるケース
		holder.setActiveBlocks(Arrays.asList(9, 7, 2, 1));
		assertEquals(Arrays.asList(1, 2, 7, 9), holder.getActiveBlocks());
		assertEquals(4, holder.getNumberOfActiveBlacks());
		assertEquals(-1, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);

		holder.addActiveBlockNumber(0);
		assertEquals(Arrays.asList(7, 9), holder.getActiveBlocks());
		assertEquals(5, holder.getNumberOfActiveBlacks());
		assertEquals(2, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		testCopied(holder);

		// 以下例外がスローされるケース
		Exception e;
		holder.setActiveBlocks(Arrays.asList(9, 7, 0, 1));
		assertEquals(Arrays.asList(7, 9), holder.getActiveBlocks());
		assertEquals(4, holder.getNumberOfActiveBlacks());
		assertEquals(1, holder.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

		// 負数をしたケース
		 e = assertThrows(IllegalArgumentException.class, () -> holder.addActiveBlockNumber(-1));
		 assertEquals("Negative value: -1", e.getMessage());
		 e = assertThrows(IllegalArgumentException.class, () -> holder.addActiveBlockNumber(-999));
		 assertEquals("Negative value: -999", e.getMessage());

		// すでに存在するブロック番号を指定したケース-リストに含まれるブロック番号
		 e = assertThrows(IllegalArgumentException.class, () -> holder.addActiveBlockNumber(7));
		 assertEquals("Already active block: 7", e.getMessage());

		 // すでに存在するブロック番号を指定したケース-先頭の連続したブロックのブロック番号
		 e = assertThrows(IllegalArgumentException.class, () -> holder.addActiveBlockNumber(0));
		 assertEquals("Already active block: 0", e.getMessage());
	}

	/**
	 * 指定のActiveBlockNumberHolderをtoStringしてvalueOfで戻した
	 * オブジェクト、およびcloneしたオブジェクトの値が、オリジナルと
	 * 同じことを確認する。
	 *
	 * @param holder
	 */
	private void testCopied(ActiveBlockNumberHolder org) {
		checkToStringAndValueOf(org, org.clone());
		checkToStringAndValueOf(org, ActiveBlockNumberHolder.valueOf(org.toString()));
	}

	private void checkToStringAndValueOf(ActiveBlockNumberHolder org
			, ActiveBlockNumberHolder copied) {
		assertIterableEquals(org.getActiveBlocks(), copied.getActiveBlocks());
		assertEquals(org.getMaximumBlockNumberOfFirstConsecutiveActiveBlock(),
				copied.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		assertEquals(org.getNumberOfActiveBlacks(), copied.getNumberOfActiveBlacks());
	}
}
