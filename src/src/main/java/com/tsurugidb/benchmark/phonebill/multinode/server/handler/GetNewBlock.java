package com.tsurugidb.benchmark.phonebill.multinode.server.handler;

import com.tsurugidb.benchmark.phonebill.multinode.server.Server.ServerTask;

public class GetNewBlock extends MessageHandlerBase {
	/**
	 * @param server このハンドラを呼び出すServerTask
	 */
	public GetNewBlock(ServerTask task) {
		super(task);
	}

	@Override
	public String getResponseString() {
		return Integer.toString(getTask().getSingleProcessContractBlockManager().getNewBlock());
	}

}
