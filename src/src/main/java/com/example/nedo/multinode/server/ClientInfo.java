package com.example.nedo.multinode.server;

import java.util.HashSet;
import java.util.Set;

import com.example.nedo.multinode.ClientType;
import com.example.nedo.multinode.NetworkIO.Message;

public class ClientInfo {
	private ClientType type;
	private Status status;
	private RequestForClient requestForClient;
	private String messageFromClient;

	// TODO コマンドラインクライアントからのリクエストと、クライアントからの通知に対して、ステータスをどう変化させるのかと、
	// コマンドラインクライアントに対してどうレスポンスするのかをマトリクスを作って実装する

	/**
	 * クライアントに対するリクエストが処理中であることを示すフラグ
	 */
	boolean requestProccessing;

	public ClientInfo(ClientType type) {
		this.type = type;
		status = Status.READY;
		requestForClient = RequestForClient.NONE;
	}

	/**
	 * リクエストに対するリクエスト状況を定義する列挙型
	 */
	/**
	 *
	 */
	public enum RequestForClient {
		NONE(Message.REQUEST_NONE), // リクエストがない状態
		SHUTDOWN(Message.REQUEST_SHUTDOWN), // プロセス終了するリクエスト
		STOP(Message.REQUEST_STOP), // バッチ/オンラインアプリの実行を停止する
		RUN(Message.REQUEST_RUN), // バッチ/オンラインアプリを実行する
		;

		/**
		 * 当該リクエストに対応するポーリングに対するレスポンス
		 */
		private Message pollingResponse;

		/**
		 * ポーリングに対するレスポンスを指定するコンストラクタ
		 *
		 * @param requestMessage
		 */
		private RequestForClient(Message pollingResponse) {
			this.pollingResponse = pollingResponse;
		}

		/**
		 * @return requestMessage
		 */
		public Message getPollingResponse() {
			return pollingResponse;
		}
	}

	/**
	 * クライアントの状態
	 */
	public enum Status {
		READY, // バッチ・オンラインアプリの実行を待っている状態
		RUNNING, // バッチ・オンラインアプリの実行中
		FAIL, // バッチ・オンラインアプリの実行でエラーが発生して終了した状態
		SUCCESS, //バッチ・オンラインアプリが正常に終了した状態
		DOWN, // 何らかの理由でサーバとの接続が切れた状態
;
		private static final Set<Status> endStatusSet = new HashSet<ClientInfo.Status>();
		static {
			endStatusSet.add(Status.FAIL);
			endStatusSet.add(Status.SUCCESS);
			endStatusSet.add(Status.DOWN);
		}

		/**
		 * @return クライアントが終了状態の時true
		 */
		public boolean isEndStatus() {
			return endStatusSet.contains(this);
		}
	}


	/**
	 * @return type
	 */
	public ClientType getType() {
		return type;
	}

	/**
	 * @return status
	 */
	public synchronized Status getStatus() {
		return status;
	}

	/**
	 * @return requestForClient
	 */
	public synchronized RequestForClient getRequestForClient() {
		return requestForClient;
	}


	/**
	 * 指定のrequestの処理開始を通知する
	 *
	 * @return 処理開始が可能な場合true, 開始できない場合はfalseを返す
	 */
	public synchronized boolean setRequestProccessing(RequestForClient request) {
		if (this.requestForClient == request && !requestProccessing && request != RequestForClient.NONE && ! status.isEndStatus()) {
			requestProccessing = true;
			return true;
		}
		return false;
	}

	/**
	 * requestProccessingフラグを落とす
	 */
	public synchronized void resetRequestProccessing() {
		requestProccessing = false;
	}

	/**
	 * @param status セットする status
	 */
	public synchronized void setStatus(Status newStatus) {
		status = newStatus;
	}

	/**
	 * @return messageFromClient
	 */
	public synchronized String getMessageFromClient() {
		return messageFromClient;
	}

	/**
	 * @param messageFromClient セットする messageFromClient
	 */
	public synchronized void setMessageFromClient(String messageFromClient) {
		this.messageFromClient = messageFromClient;
	}
}
