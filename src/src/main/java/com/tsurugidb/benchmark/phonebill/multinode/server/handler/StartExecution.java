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

import java.util.Set;
import java.util.stream.Collectors;

import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo;
import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo.RequestForClient;
import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo.Status;
import com.tsurugidb.benchmark.phonebill.multinode.server.Server.ServerTask;

public class StartExecution extends MessageHandlerBase {
	static final String MSG_NO_CLIENTS = "Failed to start execution because no clients are ready to run.";
	static final String MSG_NO_BATCH = "Failed to start execution because PhoneBillBatchClient is not ready to run.";

	public StartExecution(ServerTask task) {
		super(task);
	}

	@Override
	public String getResponseString() {
		Set<ClientInfo> clientInfos = getTask().getClientInfos().stream().filter(c -> c.getStatus() == Status.READY)
				.collect(Collectors.toSet());
		if (clientInfos.size() == 0) {
			return MSG_NO_CLIENTS;
		}
		StringBuilder sb = new StringBuilder();
		for(ClientInfo info: clientInfos) {
			info.setRequestForClient(RequestForClient.RUN);
			sb.append("Request start for a client: Type = " + info.getType() + ", Node = " + info.getNode() + "\n");
		}
		return sb.toString();
	}
}
