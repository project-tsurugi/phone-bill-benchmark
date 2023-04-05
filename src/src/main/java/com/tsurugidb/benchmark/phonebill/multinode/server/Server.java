package com.tsurugidb.benchmark.phonebill.multinode.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.ExecutableCommand;
import com.tsurugidb.benchmark.phonebill.multinode.ClientType;
import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO;
import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO.Message;
import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO.Type;
import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO.WholeMessage;
import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo.Status;
import com.tsurugidb.benchmark.phonebill.multinode.server.handler.GetActiveBlockInfo;
import com.tsurugidb.benchmark.phonebill.multinode.server.handler.GetClusterStatus;
import com.tsurugidb.benchmark.phonebill.multinode.server.handler.GetNewBlock;
import com.tsurugidb.benchmark.phonebill.multinode.server.handler.InitOnlineApp;
import com.tsurugidb.benchmark.phonebill.multinode.server.handler.InitPhoneBill;
import com.tsurugidb.benchmark.phonebill.multinode.server.handler.MessageHandlerBase;
import com.tsurugidb.benchmark.phonebill.multinode.server.handler.Polling;
import com.tsurugidb.benchmark.phonebill.multinode.server.handler.ShutdownCluster;
import com.tsurugidb.benchmark.phonebill.multinode.server.handler.StartExecution;
import com.tsurugidb.benchmark.phonebill.multinode.server.handler.SubmitBlock;
import com.tsurugidb.benchmark.phonebill.multinode.server.handler.UpdateStatus;
import com.tsurugidb.benchmark.phonebill.testdata.DbContractBlockInfoInitializer;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class Server extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    /**
     * シャットダウン要求の有無を表すフラグ
     */
    private AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    /**
     * サーバプロセス内のスレッドを管理するExecutorService
     */
    private ExecutorService service = Executors.newCachedThreadPool();

    /**
     * 実行中のクライアント情報のセット
     * <p>
     * 実行中のタスクを制御するためにクライアント情報を保持する。コマンドライン用のクライアントは
     * このセットの管理対象外である。
     *
     */
    private Set<ClientInfo> clientInfos = Collections.synchronizedSet(new LinkedHashSet<>());

    /**
     * 料金計算バッチ用クライアントの存在を表すフラグ
     */
    private AtomicBoolean phoneBillClientAlive = new AtomicBoolean(false);

    /**
     * 契約マスタのブロック情報管理
     */
    private SingleProcessContractBlockManager singleProcessContractBlockManager = null;


    /**
     * スレッド名の連番の採番用変数
     */
    private AtomicInteger threadNumber = new AtomicInteger(0);

    /**
     * Config値
     */
    private Config config = null;


	public static void main(String[] args) throws Exception {
		Server server = new Server();
		Config config = Config.getConfig(args);
		server.execute(config);
	}


	/**
	 * コマンドを実行する
	 * @throws InterruptedException
	 */
	@Override
	public void execute(Config config) throws InterruptedException  {
		this.config = config.clone();
		LOG.info("Starting server...");

		DbContractBlockInfoInitializer initializer = new DbContractBlockInfoInitializer(config);
		singleProcessContractBlockManager = new SingleProcessContractBlockManager(initializer);

		ListenerTask listenerTask = new ListenerTask();
		Future<?> future = service.submit(listenerTask);
		while (!future.isDone()) {
			try {
				future.get(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				continue;
			} catch (ExecutionException e) {
				break;
			} catch (TimeoutException e) {
				if (shutdownRequested.get()) {
					future.cancel(true);
				}
			}
		}
		service.shutdown();
		service.awaitTermination(5, TimeUnit.MINUTES);
		LOG.info("Server terminating normally.");
	}

	/**
	 * クライアントからの接続要求を処理するタスク
	 *
	 */
	@SuppressFBWarnings(value="RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
	public class ListenerTask implements Runnable {
		@Override
		public void run() {
			Thread.currentThread().setName("SocketListener");
			try (ServerSocket serverSocket = new ServerSocket(config.listenPort, 100)) {
				LOG.info("Listening port {}", config.listenPort);
				serverSocket.setSoTimeout(1000);
				for (;;) {
					try {
						Socket socket = serverSocket.accept();
						ServerTask task = new ServerTask(socket);
						service.submit(task);
					} catch (SocketTimeoutException e) {
						if (shutdownRequested.get()) {
							break;
						}
					}
				}
			} catch (IOException e) {
				LOG.warn("An exception was caught in the listener task. The process will be terminated.", e);
			}
		}

	}

	/**
	 * アクセプトしたソケットを処理するタスク.
	 *
	 * <ol>
	 *   <li> クライアントからのリクエストを待ちクライアントの種別を判別する
	 *   <li> 常時接続クライアントに対しては初期化処理後クライアントからのリクエストを処理する
	 *     <ul>
	 *         <li> オンラインアプリケーション用クライアントは無条件に受け入れる
	 *         <li> オンラインバッチアプリケーションは2つ以上は受け入れない
	 *     <ul>
	 *   <li> コマンドラインクライアントに対しては、コマンドを実行し処理結果を標準出力に出力して終了する。
	 * </ol>
	 */
	/**
	 *
	 */
	public class ServerTask implements Runnable {
		// 通信に使用するソケット
		private Socket socket;

		// クライアントの情報
		private ClientInfo clientInfo;

		// メッセージとメッセージハンドラのマップ
		private Map<Message, MessageHandlerBase> map = new HashMap<NetworkIO.Message, MessageHandlerBase>();


		public ServerTask(Socket socket) {
			this.socket = socket;
			initMessageHandler();
			clientInfo = null;
		}

		/**
		 * メッセージハンドラを初期化しメッセージとメッセージハンドラのマップに格納する
		 */
		private void initMessageHandler() {
			// クライアントの初期化
			map.put(Message.INIT_ONLINE_APP, new InitOnlineApp(this));
			map.put(Message.INIT_PHONE_BILL, new InitPhoneBill(this));

			// クライアントからのポーリング
			map.put(Message.POLLING, new Polling(this));

			// クライアントからのステータス変更通知
			map.put(Message.UPDATE_STATUS, new UpdateStatus(this));

			// 契約マスタのブロック情報の処理
			map.put(Message.GET_NEW_BLOCK, new GetNewBlock(this));
			map.put(Message.SUBMIT_BLOCK, new SubmitBlock(this));
			map.put(Message.GET_ACTIVE_BLOCK_INFO, new GetActiveBlockInfo(this));

			// コマンドラインクライアントのコマンド
			map.put(Message.GET_CLUSTER_STATUS, new GetClusterStatus(this));
			map.put(Message.START_EXECUTION, new StartExecution(this));
			map.put(Message.SHUTDOWN_CLUSTER, new ShutdownCluster(this, shutdownRequested));
		}

		@Override
		public void run() {
			int num = threadNumber.getAndAdd(1);
			Thread.currentThread().setName("ServerTask-" + num);
			try(NetworkIO io = new NetworkIO(socket)) {
				WholeMessage received;
				while ((received = io.recieveFromClient()) != null) {
					MessageHandlerBase handler = map.get(received.message);
					if (handler == null) {
						throw new IOException("Protocol error, bad message: " + received.message.name());
					}
					handler.handle(received.body);
					Type type = received.message.getType();
					if (type == Type.POLLING) {
						io.response(handler.getPollingResponse());
					} else if (type == Type.REQUEST) {
						io.response(handler.getResponseString());
					}
					// ステータスが終了ステータスになった => クライアントの終了通知
					if (clientInfo != null && clientInfo.getStatus().isEndStatus()) {
						LOG.info("Receive a message the client has finished from node: {}.", clientInfo.getNode());
						break;
					}
				}
				if (clientInfo != null && !clientInfo.getStatus().isEndStatus()) {
					// 終了ステータスになっていないのに接続が切れた場合
					clientInfo.setStatus(Status.DOWN);
					clientInfo.setMessageFromClient("Connection closed by the client.");
					LOG.info("Connection closed by the clien at node: {}.", clientInfo.getNode());
				}
			} catch (IOException | RuntimeException e) {
				LOG.warn("IO error, aborting server task....", e);
			}
			if (clientInfo != null && clientInfo.getType() ==ClientType.PHONEBILL) {
				getPhoneBillClientAlive().set(false);
			}
		}

		/**
		 * @return phoneBillClientAlive
		 */
		public AtomicBoolean getPhoneBillClientAlive() {
			return phoneBillClientAlive;
		}


		/**
		 * @return config
		 */
		public Config getConfig() {
			return config;
		}

		/**
		 * クライアント情報を登録する
		 */
		public void setClientInfo(ClientInfo clientInfo) {
			if (this.clientInfo != null) {
				throw new RuntimeException("ClientInfo already setten.");
			}
			this.clientInfo = new ClientInfo(clientInfo);
			if (clientInfo.getType() != ClientType.COMMAND_LINE) {
				this.clientInfo.setNode(socket.getInetAddress().getHostName());
				this.clientInfo.setStart(Instant.now());
				clientInfos.add(this.clientInfo);
			}
		}


		/**
		 * @return singleProcessContractBlockManager
		 */
		public SingleProcessContractBlockManager getSingleProcessContractBlockManager() {
			return singleProcessContractBlockManager;
		}

		/**
		 * @return clientInfo
		 */
		public ClientInfo getClientInfo() {
			return clientInfo;
		}

		/**
		 * @return clientInfos
		 */
		public Set<ClientInfo> getClientInfos() {
			return clientInfos;
		}
	}
}
