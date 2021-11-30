package com.example.nedo.multinode.server.handler;

import java.util.Set;
import java.util.stream.Collectors;

import com.example.nedo.multinode.server.ClientInfo;
import com.example.nedo.multinode.server.ClientInfo.RequestForClient;
import com.example.nedo.multinode.server.ClientInfo.Status;
import com.example.nedo.multinode.server.Server.ServerTask;

public class StartExecution extends MessageHandlerBase {
	static final String MSG_NO_CLIENTS = "Failed to start execution because no clients are ready to run.";
	static final String MSG_NO_BATCH = "Failed to start execution because PhoneBillBatchClient is not ready to run.";

	public StartExecution(ServerTask task) {
		super(task);
	}

	@Override
	public String getResponseString() {
		Set<ClientInfo> clientInfos = getTask().getClientInfos().stream().filter(c -> c.getStatus() == Status.READY)
				.collect(Collectors.toSet());
		if (clientInfos.size() == 0) {
			return MSG_NO_CLIENTS;
		}
		StringBuilder sb = new StringBuilder();
		for(ClientInfo info: clientInfos) {
			info.setRequestForClient(RequestForClient.RUN);
			sb.append("Request start for a client: Type = " + info.getType() + ", Node = " + info.getNode() + "\n");
		}
		return sb.toString();
	}
}
