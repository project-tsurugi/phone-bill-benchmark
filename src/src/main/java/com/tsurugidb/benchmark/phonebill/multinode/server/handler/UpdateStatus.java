package com.tsurugidb.benchmark.phonebill.multinode.server.handler;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo;
import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo.Status;
import com.tsurugidb.benchmark.phonebill.multinode.server.Server.ServerTask;

public class UpdateStatus extends MessageHandlerBase {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateStatus.class);

	/**
	 * @param server このハンドラを呼び出すServerTask
	 */
	public UpdateStatus(ServerTask task) {
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
		LOG.info("Client status updated: {}, {}, {}", info.getType(), info.getNode(), info.getMessageFromClient());
	}
}
