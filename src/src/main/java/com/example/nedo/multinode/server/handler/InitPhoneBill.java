package com.example.nedo.multinode.server.handler;

import java.io.IOException;
import java.util.List;

import com.example.nedo.multinode.ClientType;
import com.example.nedo.multinode.server.ClientInfo;
import com.example.nedo.multinode.server.Server.ServerTask;

public class InitPhoneBill extends MessageHandlerBase {
	/**
	 * @param server このハンドラを呼び出すServerTask
	 */
	public InitPhoneBill(ServerTask task) {
		super(task);
	}

	@Override
	public void handle(List<String> msgBody) throws IOException {
		if (!getTask().getPhoneBillClientAlive().compareAndSet(false, true)) {
			throw new IOException("A phone bill batch client has already connected.");
		}
		ClientInfo clientInfo = new ClientInfo(ClientType.PHNEBILL);
		getTask().setClientInfo(clientInfo);
	}

	@Override
	public String getResponseString() {
		return getTask().getConfig().toString();
	}
}
