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