package com.example.nedo.multinode.client;

import java.io.IOException;

import com.example.nedo.multinode.NetworkIO;
import com.example.nedo.multinode.NetworkIO.Message;
import com.example.nedo.testdata.ActiveBlockNumberHolder;
import com.example.nedo.testdata.ContractBlockInfoAccessor;

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
		io.notifyMessage(Message.SUBMIT_BLOCK);
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