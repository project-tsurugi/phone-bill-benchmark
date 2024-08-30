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
package com.tsurugidb.benchmark.phonebill.db;

public enum TxLabel {
    BATCH_MAIN,
    ONLINE_MASTER_DELETE,
    ONLINE_MASTER_INSERT,
    ONLINE_MASTER_UPDATE,
    ONLINE_HISTORY_INSERT,
    ONLINE_HISTORY_UPDATE,
    BATCH_INITIALIZE,
    INITIALIZE,
    TEST_DATA_GENERATOR,
    CHECK_RESULT,
    DDL,
    TEST,
    DEFAULT,
}
