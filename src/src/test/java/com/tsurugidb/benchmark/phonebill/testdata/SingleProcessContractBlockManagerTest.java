/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.benchmark.phonebill.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

class SingleProcessContractBlockManagerTest {
	@Test
	void test() {
		SingleProcessContractBlockManager manager = new SingleProcessContractBlockManager();

		// 20ブロック取得
		Set<Integer> expectedWaitingBlocks = new HashSet<>();
		for (int i = 0; i < 20; i++) {
			assertEquals(i, manager.getNewBlock());
			assertEquals(i + 1, manager.getNumberOfBlacks());
			expectedWaitingBlocks.add(i);
			assertEquals(expectedWaitingBlocks, manager.getWaitingBlocks());
			ActiveBlockNumberHolder info = manager.getActiveBlockInfo();
			assertEquals(0, info.getNumberOfActiveBlacks());
			assertEquals(Collections.emptyList(), info.getActiveBlocks());
			assertEquals(-1, info.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		}

		// 10ブロックをアクティブなブロックにする
		int expectedNumberOfBlack = 20;
		int numberOfActiveBlacks = 0;
		List<Integer> activatingBlocks =Arrays.asList(1, 0, 6, 2, 8, 7, 15, 3, 19, 11 );
		for (int n: activatingBlocks) {
			manager.submit(n);
			assertEquals(expectedNumberOfBlack, manager.getNewBlock());
			expectedWaitingBlocks.remove(n);
			expectedWaitingBlocks.add(expectedNumberOfBlack++);
			assertEquals(expectedWaitingBlocks, manager.getWaitingBlocks());
			assertEquals(expectedNumberOfBlack, manager.getNumberOfBlacks());
			ActiveBlockNumberHolder info = manager.getActiveBlockInfo();
			assertEquals(++numberOfActiveBlacks, info.getNumberOfActiveBlacks());
		}
		Set<Integer> sortedExpectedActiveBlocks = new TreeSet<Integer>(activatingBlocks);
		sortedExpectedActiveBlocks.remove(1);
		sortedExpectedActiveBlocks.remove(0);
		sortedExpectedActiveBlocks.remove(2);
		sortedExpectedActiveBlocks.remove(3);
		ActiveBlockNumberHolder info = manager.getActiveBlockInfo();
		assertIterableEquals(sortedExpectedActiveBlocks, info.getActiveBlocks());
		assertEquals(3, info.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());

		// さらに20ブロック取得
		for (int i = 30; i < 50; i++) {
			assertEquals(i, manager.getNewBlock());
			assertEquals(i + 1, manager.getNumberOfBlacks());
			expectedWaitingBlocks.add(i);
			assertEquals(expectedWaitingBlocks, manager.getWaitingBlocks());
			assertEquals(numberOfActiveBlacks, info.getNumberOfActiveBlacks());
			assertIterableEquals(sortedExpectedActiveBlocks, info.getActiveBlocks());
			assertEquals(3, info.getMaximumBlockNumberOfFirstConsecutiveActiveBlock());
		}

		// getNewBlock(int n)に不正な値を指定
		Exception e;
		e = assertThrows(IllegalArgumentException.class, () -> manager.submit(-1));
		assertEquals("Not waiting blocks, block number = -1", e.getMessage());
		e = assertThrows(IllegalArgumentException.class, () -> manager.submit(6));
		assertEquals("Not waiting blocks, block number = 6", e.getMessage());
		e = assertThrows(IllegalArgumentException.class, () -> manager.submit(Integer.MAX_VALUE));
		assertEquals("Not waiting blocks, block number = " + Integer.MAX_VALUE, e.getMessage());
	}

}
