package com.example.nedo.multinode.server.handler;

import com.example.nedo.multinode.server.Server.ServerTask;

public class GetActiveBlockInfo extends MessageHandlerBase {
	/**
	 * @param server このハンドラを呼び出すServerTask
	 */
	public GetActiveBlockInfo(ServerTask task) {
		super(task);
	}

	@Override
	public String getResponseString() {
		return getTask().getSingleProcessContractBlockManager().getActiveBlockInfo().toString();
	}
}
