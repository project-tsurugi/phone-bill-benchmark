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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo;
import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo.Status;
import com.tsurugidb.benchmark.phonebill.multinode.server.Server.ServerTask;

public class UpdateStatus extends MessageHandlerBase {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateStatus.class);

	/**
	 * @param server このハンドラを呼び出すServerTask
	 */
	public UpdateStatus(ServerTask task) {
		super(task);
	}

	@Override
	public void handle(List<String> msgBody) throws IOException {
		// クライアント情報を取得
		ClientInfo info = getTask().getClientInfo();
		// メッセージを改行で分割
		if (msgBody.size() != 2) {
			throw new IOException("Illegal message body: " + String.join("\n", msgBody) );
		}

		// msgBodyの1行目はクライアントのステータス
		String status = msgBody.get(0);
		try {
			info.setStatus(Status.valueOf(status));
		} catch (IllegalStateException e) {
			throw new IOException("Protocol error, illegal status: " + status);
		}
		// msgBodyの2行目がクライアントからのメッセージ
		info.setMessageFromClient(msgBody.get(1));
		LOG.info("Client status updated: {}, {}, {}", info.getType(), info.getNode(), info.getMessageFromClient());
	}
}
