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
package com.tsurugidb.benchmark.phonebill.multinode.server.handler;

import java.io.IOException;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.multinode.ClientType;
import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo;
import com.tsurugidb.benchmark.phonebill.multinode.server.Server.ServerTask;

public class InitOnlineApp extends MessageHandlerBase {
	/**
	 * @param server このハンドラを呼び出すServerTask
	 */
	public InitOnlineApp(ServerTask task) {
		super(task);
	}

	@Override
	public void handle(List<String> msgBody) throws IOException {
		ClientInfo clientInfo = new ClientInfo(ClientType.ONLINE_APP);
		getTask().setClientInfo(clientInfo);
	}

	@Override
	public String getResponseString() {
		return getTask().getConfig().toString();
	}
}
