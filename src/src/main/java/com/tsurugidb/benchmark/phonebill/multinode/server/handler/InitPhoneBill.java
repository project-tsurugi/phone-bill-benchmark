package com.tsurugidb.benchmark.phonebill.multinode.server.handler;

import java.io.IOException;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.multinode.ClientType;
import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo;
import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo.RequestForClient;
import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo.Status;
import com.tsurugidb.benchmark.phonebill.multinode.server.Server.ServerTask;

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
		ClientInfo clientInfo = new ClientInfo(ClientType.PHONEBILL, handler);
		getTask().setClientInfo(clientInfo);
	}

	@Override
	public String getResponseString() {
		return getTask().getConfig().toString();
	}

	/**
	 * ステータス変更時に呼び出されるハンドラ
	 */
	private StatusChangeEventHandler handler = new StatusChangeEventHandler() {
		@Override
		public void handleStatusChangeEvent(Status oldStatus, Status newStatus) {
			// ステータスが終了ステータスに変更されたら、オンラインアプリケーションを停止する
			if (!oldStatus.isEndStatus() && newStatus.isEndStatus()) {
				for (ClientInfo info : getTask().getClientInfos()) {
					if (info.getType() == ClientType.ONLINE_APP) {
						info.setRequestForClient(RequestForClient.STOP);
					}
				}
			}
		}
	};

}
