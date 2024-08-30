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
package com.tsurugidb.benchmark.phonebill.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * テスト用のRandomクラス
 * <p>
 * nextInt()が返す値をsetValues()で指定できる。
 */
@SuppressFBWarnings("SE_NO_SERIALVERSIONID")
public class TestRandom extends Random {
    Queue<Integer> queue = new LinkedList<>();

    public void setValues(Integer... integers) {
        setValues(Arrays.asList(integers));
    }

    public void setValues(Collection<Integer> integers) {
        queue.clear();
        queue.addAll(integers);
    }

    @Override
    public int nextInt() {
        return queue.poll();
    }

    @Override
    public int nextInt(int bound) {
        return nextInt();
    }
}
