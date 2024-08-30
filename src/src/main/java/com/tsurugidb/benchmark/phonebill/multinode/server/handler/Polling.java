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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO.Message;
import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo;
import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo.RequestForClient;
import com.tsurugidb.benchmark.phonebill.multinode.server.Server.ServerTask;

public class Polling extends MessageHandlerBase {
    private static final Logger LOG = LoggerFactory.getLogger(Polling.class);

	/**
	 * @param server このハンドラを呼び出すServerTask
	 */
	public Polling(ServerTask task) {
		super(task);
	}

	@Override
	public Message getPollingResponse() {
		ClientInfo info = getTask().getClientInfo();
		RequestForClient request = info.getRequestForClient();
		if (request == null) {
			return Message.REQUEST_NONE;
		}
		LOG.info("Send request {} to client type = {}, node = {}, request = {}", request.name(), info.getType(),
				info.getNode());
		return request.getPollingResponse();
	}
}
