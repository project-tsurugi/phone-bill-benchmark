package com.example.nedo.multinode.server.handler;

import java.io.IOException;
import java.util.List;

import com.example.nedo.multinode.NetworkIO.Message;
import com.example.nedo.multinode.server.ClientInfo;
import com.example.nedo.multinode.server.ClientInfo.RequestForClient;
import com.example.nedo.multinode.server.ClientInfo.Status;
import com.example.nedo.multinode.server.Server.ServerTask;

public class Polling extends MessageHandlerBase {
	/**
	 * @param server このハンドラを呼び出すServerTask
	 */
	public Polling(ServerTask task) {
		super(task);
	}

	@Override
	public void handle(List<String> msgBody) throws IOException {
		// クライアント情報を取得
		ClientInfo info = getTask().getClientInfo();
		// メッセージを改行で分割
		if (msgBody.size() != 2) {
			throw new IOException("Illegal message body: " + String.join("\n", msgBody) );
		}

		// msgBodyの1行目はクライアントのステータス
		String status = msgBody.get(0);
		try {
			info.setStatus(Status.valueOf(status));
		} catch (IllegalStateException e) {
			throw new IOException("Protocol error, illegal status: " + status);
		}
		// msgBodyの2行目がクライアントからのメッセージ
		info.setMessageFromClient(msgBody.get(1));
	}

	@Override
	public Message getPollingResponse() {
		ClientInfo info = getTask().getClientInfo();
		RequestForClient request = info.getRequestForClient();
		if (info.setRequestProccessing(request)) {
			return request.getPollingResponse();
		}
		return Message.REQUEST_RUN;
	}
}
