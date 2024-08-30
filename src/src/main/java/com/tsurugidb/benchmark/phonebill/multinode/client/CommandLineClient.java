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
package com.tsurugidb.benchmark.phonebill.multinode.client;

import java.net.Socket;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.app.ExecutableCommand;
import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO;
import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO.Message;

/**
 * コマンドラインクライアント
 */
public class CommandLineClient extends ExecutableCommand {
	/**
	 * サーバに送信するメッセージ
	 */
	private Message message;

	/**
	 * コンストラクタ
	 *
	 * @param message サーバに送信するメッセージ
	 */
	public CommandLineClient(Message message) {
		this.message = message;
	}

	public static void main(String[] args) throws Exception {
		String hostname = args[0].replaceAll(":.*", "");
		int port = Integer.parseInt(args[0].replaceAll(".*:", ""));
		CommandLineClient client = new CommandLineClient(Message.valueOf(args[1]));
		client.execute(hostname, port);
	}

	@Override
	public void execute(String hostname, int port) throws Exception {
		Socket socket = new Socket(hostname, port);
		try (NetworkIO io = new NetworkIO(socket)) {
			List<String> response = io.request(message);
			for (String line : response) {
				System.out.println(line);
			}
		}
	}
}
