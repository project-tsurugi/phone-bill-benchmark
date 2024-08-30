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

import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO.Message;
import com.tsurugidb.benchmark.phonebill.multinode.server.Server.ServerTask;

/**
 * クライアントからのメッセージを処理するメッセージハンドラの基底クラス
 */
public class MessageHandlerBase {
	/**
	 * ハンドラを使用するServerTask
	 */
	private ServerTask task;


	MessageHandlerBase(ServerTask task) {
		this.task = task;
	}


	/**
	 * 受信したメッセージを処理する
	 *
	 * @param body 受信したメッセージのメッセージ本体
	 * @throws IOException
	 */
	public void handle(List<String> body) throws IOException {
		// デフォルト実装ではなにもしない。必要に応じてサブクラスがオーバーライドする。
	}

	/**
	 * ポーリングのレスポンスを返す.
	 *
	 * @return
	 */
	public Message getPollingResponse() {
		// デフォルト実装ではなnullを返す。必要に応じてサブクラスがオーバーライドする。
		return null;
	}

	/**
	 * リクエストに対する応答の文字列を返す.
	 *
	 * @return
	 */
	public String getResponseString() {
		// デフォルト実装ではなnullを返す。必要に応じてサブクラスがオーバーライドする。
		return null;
	}


	/**
	 * このハンドラを使用するTaskを返す
	 *
	 * @return task
	 */
	protected final ServerTask getTask() {
		return task;
	}


}
