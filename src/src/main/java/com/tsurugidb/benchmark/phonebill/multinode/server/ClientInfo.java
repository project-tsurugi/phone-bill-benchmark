package com.tsurugidb.benchmark.phonebill.multinode.server;

import java.time.Instant;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.multinode.ClientType;
import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO.Message;
import com.tsurugidb.benchmark.phonebill.multinode.server.handler.StatusChangeEventHandler;

public class ClientInfo {
    private static final Logger LOG = LoggerFactory.getLogger(ClientInfo.class);

	private ClientType type;
	private Status status;
	private Queue<RequestForClient> requestForClientQueue;
	private String messageFromClient;
	private String node;
	private StatusChangeEventHandler statusChangeEventHandler;
	private Instant start;

	public ClientInfo(ClientInfo c) {
		this.type = c.type;
		this.status = c.status;
		this.requestForClientQueue = c.requestForClientQueue;
		this.messageFromClient = c.messageFromClient;
		this.node = c.node;
		this.statusChangeEventHandler = c.statusChangeEventHandler;
		this.start = c.start;
	}


	public ClientInfo(ClientType type, StatusChangeEventHandler statusChangeEventHandler) {
		this.type = type;
		status = Status.INITIALIZING;
		requestForClientQueue = new ConcurrentLinkedQueue<ClientInfo.RequestForClient>();
		messageFromClient = "No message";
		this.statusChangeEventHandler = statusChangeEventHandler;
	}

	public ClientInfo(ClientType type) {
		this(type, StatusChangeEventHandler.NULL_HANDLER);
	}

	/**
	 * クライアントに対するリクエストを定義する列挙型
	 */
	/**
	 *
	 */
	public enum RequestForClient {
		NONE(Message.REQUEST_NONE), // リクエストがない状態
		STOP(Message.REQUEST_STOP), // プロセス終了するリクエスト
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
		INITIALIZING, // バッチ・オンラインアプリの初期化中
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
		RequestForClient requestForClient = requestForClientQueue.poll();
		LOG.debug("take a request {} from queue." , requestForClient);
		return requestForClient;
	}

	/**
	 * @param status セットする status
	 */
	public synchronized void setStatus(Status newStatus) {
		if (status != newStatus) {
			statusChangeEventHandler.handleStatusChangeEvent(status, newStatus);
		}
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

	/**
	 * @return node
	 */
	public String getNode() {
		return node;
	}

	/**
	 * @param node セットする node
	 */
	public void setNode(String node) {
		this.node = node;
	}

	/**
	 * @param requestForClient セットする requestForClient
	 */
	public  void setRequestForClient(RequestForClient requestForClient) {
		LOG.debug("add a request {} to queue." , requestForClient);
		requestForClientQueue.add(requestForClient);
	}

	/**
	 * @return start
	 */
	public Instant getStart() {
		return start;
	}

	/**
	 * @param start セットする start
	 */
	public void setStart(Instant start) {
		this.start = start;
	}
}
