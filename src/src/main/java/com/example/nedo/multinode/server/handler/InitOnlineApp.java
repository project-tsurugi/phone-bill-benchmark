package com.example.nedo.multinode.server.handler;

import java.io.IOException;
import java.util.List;

import com.example.nedo.multinode.ClientType;
import com.example.nedo.multinode.server.ClientInfo;
import com.example.nedo.multinode.server.Server.ServerTask;

public class InitOnlineApp extends MessageHandlerBase {
	/**
	 * @param server このハンドラを呼び出すServerTask
	 */
	public InitOnlineApp(ServerTask task) {
		super(task);
	}

	@Override
	public void handle(List<String> msgBody) throws IOException {
		ClientInfo clientInfo = new ClientInfo(ClientType.ONLINE_APP);
		getTask().setClientInfo(clientInfo);
	}

	@Override
	public String getResponseString() {
		return getTask().getConfig().toString();
	}
}
