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
package com.tsurugidb.benchmark.phonebill.app.billing;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.CrashDumper;
import com.tsurugidb.benchmark.phonebill.app.ExecutableCommand;
import com.tsurugidb.benchmark.phonebill.online.AbstractOnlineApp;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.DbContractBlockInfoInitializer;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;

public class OnlineApp extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(OnlineApp.class);

    public static void main(String[] args) throws Exception {
        Config config = Config.getConfig(args);
        OnlineApp phoneBill = new OnlineApp();
        CrashDumper.enable();
        phoneBill.execute(config);
        CrashDumper.disable();
    }

    @Override
    public void execute(Config config) throws Exception {
        DbContractBlockInfoInitializer initializer = new DbContractBlockInfoInitializer(config);
        ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager(initializer);

        List<AbstractOnlineApp> list = PhoneBill.createOnlineApps(config, accessor);
        if (list.isEmpty()) {
            LOG.info("No online applications started.");
            return;
        }
        final ExecutorService service = list.isEmpty() ? null : Executors.newFixedThreadPool(list.size());
        list.parallelStream().forEach(task -> service.submit(task));
        LOG.info("Online applications started.");
    }
}
