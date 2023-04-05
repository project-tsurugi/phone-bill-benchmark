package com.tsurugidb.benchmark.phonebill.multinode.server.handler;

import java.io.IOException;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.multinode.server.Server.ServerTask;

public class SubmitBlock extends MessageHandlerBase {
	/**
	 * @param server このハンドラを呼び出すServerTask
	 */
	public SubmitBlock(ServerTask task) {
		super(task);
	}

	@Override
	public void handle(List<String> msgBody) throws IOException {
		int blockNumber = Integer.parseInt(msgBody.get(0));
		getTask().getSingleProcessContractBlockManager().submit(blockNumber);
	}
}
