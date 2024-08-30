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

import java.io.IOException;

import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO;
import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO.Message;
import com.tsurugidb.benchmark.phonebill.testdata.ActiveBlockNumberHolder;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;

/**
 * サーバから契約マスタのブロック情報を取得するContractBlockInfoAccessorの実装
 */
final class ContractBlockInfoAccessorClient implements ContractBlockInfoAccessor {
	private final NetworkIO io;

	ContractBlockInfoAccessorClient(NetworkIO io) {
		this.io = io;
	}

	@Override
	public void submit(int blockNumber) throws IOException {
		io.notifyMessage(Message.SUBMIT_BLOCK, Integer.toString(blockNumber));
	}

	@Override
	public int getNewBlock() throws IOException {
		return Integer.parseInt(io.request(Message.GET_NEW_BLOCK).get(0));
	}

	@Override
	public ActiveBlockNumberHolder getActiveBlockInfo() throws IOException {
		return ActiveBlockNumberHolder.valueOf(io.request(Message.GET_ACTIVE_BLOCK_INFO).get(0));
	}
}