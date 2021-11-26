package com.example.nedo.multinode.server.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.multinode.NetworkIO.Message;
import com.example.nedo.multinode.server.ClientInfo;
import com.example.nedo.multinode.server.ClientInfo.RequestForClient;
import com.example.nedo.multinode.server.Server.ServerTask;

public class Polling extends MessageHandlerBase {
    private static final Logger LOG = LoggerFactory.getLogger(Polling.class);

	/**
	 * @param server このハンドラを呼び出すServerTask
	 */
	public Polling(ServerTask task) {
		super(task);
	}

	@Override
	public Message getPollingResponse() {
		ClientInfo info = getTask().getClientInfo();
		RequestForClient request = info.getRequestForClient();
		if (request == null) {
			return Message.REQUEST_NONE;
		}
		LOG.info("Send request {} to client type = {}, node = {}, request = {}", request.name(), info.getType(),
				info.getNode());
		return request.getPollingResponse();
	}
}
