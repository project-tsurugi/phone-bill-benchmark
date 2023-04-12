package com.tsurugidb.benchmark.phonebill.multinode.server.handler;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo;
import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo.RequestForClient;
import com.tsurugidb.benchmark.phonebill.multinode.server.Server.ServerTask;

public class ShutdownCluster extends MessageHandlerBase {
	private AtomicBoolean shutdownRequested;

	public ShutdownCluster(ServerTask task, AtomicBoolean shutdownRequested) {
		super(task);
		this.shutdownRequested = shutdownRequested;
	}


	@Override
	public String getResponseString() {
		shutdownRequested.set(true);
		Set<ClientInfo> clientInfos = getTask().getClientInfos().stream().filter(c -> !c.getStatus().isEndStatus())
				.collect(Collectors.toSet());
		StringBuilder sb = new StringBuilder();
		for(ClientInfo info: clientInfos) {
			info.setRequestForClient(RequestForClient.STOP);
			sb.append("Request stop for a client: Type = " + info.getType() + ", Node = " + info.getNode() + "\n");
		}
		return sb.toString();
	}
}
