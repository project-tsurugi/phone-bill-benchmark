package com.tsurugidb.benchmark.phonebill.multinode.server.handler;

import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo.Status;

/**
 * クライアントのステータスが変化したときに呼び出されるハンドラ
 */
/**
 *
 */
public interface StatusChangeEventHandler {
	void handleStatusChangeEvent(Status oldStatus, Status newStatus);

	/**
	 * 何もしないハンドラのインスタンス
	 *
	 * @return
	 */
	public static final StatusChangeEventHandler NULL_HANDLER = new StatusChangeEventHandler() {
		@Override
		public void handleStatusChangeEvent(Status oldStatus, Status newStatus) {
		}
	};
}
