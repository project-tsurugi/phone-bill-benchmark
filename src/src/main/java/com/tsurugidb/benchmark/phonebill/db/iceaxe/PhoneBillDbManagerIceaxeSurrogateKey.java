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
package com.tsurugidb.benchmark.phonebill.db.iceaxe;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.DdlIceaxeSurrogateKey;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.HistoryDaoIceaxeSurrogateKey;

public class PhoneBillDbManagerIceaxeSurrogateKey extends PhoneBillDbManagerIceaxe {

    public PhoneBillDbManagerIceaxeSurrogateKey(Config config) {
        super(config, InsertType.UPSERT);
    }

    private Ddl ddl;

    @Override
    public Ddl getDdl() {
        if (ddl == null) {
            ddl = new DdlIceaxeSurrogateKey(this);
        }
        return ddl;
    }

    private HistoryDao historyDao;

    @Override
    public HistoryDao getHistoryDao() {
        if (historyDao == null) {
            historyDao = new HistoryDaoIceaxeSurrogateKey(this);
        }
        return historyDao;
    }

}
