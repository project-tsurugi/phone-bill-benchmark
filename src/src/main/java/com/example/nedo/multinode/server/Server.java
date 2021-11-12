package com.example.nedo.multinode.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.app.ExecutableCommand;
import com.example.nedo.multinode.ClientType;
import com.example.nedo.multinode.NetworkIO;
import com.example.nedo.multinode.NetworkIO.Message;
import com.example.nedo.multinode.NetworkIO.Type;
import com.example.nedo.multinode.NetworkIO.WholeMessage;
import com.example.nedo.multinode.server.Server.ListenerTask.Result;
import com.example.nedo.multinode.server.handler.MessageHandlerBase;
import com.example.nedo.multinode.server.handler.GetActiveBlockInfo;
import com.example.nedo.multinode.server.handler.GetNewBlock;
import com.example.nedo.multinode.server.handler.InitCommandLineClient;
import com.example.nedo.multinode.server.handler.InitOnlineApp;
import com.example.nedo.multinode.server.handler.InitPhoneBill;
import com.example.nedo.multinode.server.handler.Polling;
import com.example.nedo.multinode.server.handler.SubmitBlock;
import com.example.nedo.testdata.DbContractBlockInfoInitializer;
import com.example.nedo.testdata.SingleProcessContractBlockManager;

public class Server extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    /**
     * サーバプロセス内のスレッドを管理するExecutorService
     */
    private ExecutorService service = Executors.newCachedThreadPool();

    /**
     * 実行中のタスクのセット.
     * <p>
     * 実行中のタスクを制御するために、タスクのセットを保持する。制御対象ではない、CommandLineTask用の
     * ServerTaskはこのセットの管理対象外である。
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
	 */
	@Override
	public void execute(Config config) throws Exception {
		this.config = config;
		LOG.info("Starting server...");

		DbContractBlockInfoInitializer initializer = new DbContractBlockInfoInitializer(config);
		singleProcessContractBlockManager = new SingleProcessContractBlockManager(initializer);

		ListenerTask listenerTask = new ListenerTask();
		Future<Result> future =  service.submit(listenerTask);
		// TODO 終了処理を書く
		Result result =  future.get();
	}


	/**
	 * クライアントからの接続要求を処理するタスク
	 *
	 */
	public class ListenerTask implements Callable<Result> {

		@Override
		public Result call() throws IOException {
			Thread.currentThread().setName("SocketListener");
			try (ServerSocket serverSocket = new ServerSocket(config.listenPort)) {
				LOG.info("Listening port {}", config.listenPort);
				for (;;) {
					Socket socket = serverSocket.accept();
					ServerTask task = new ServerTask(socket);
					// TODO 例外処理を入れる
					Future<?> future =service.submit(task);
				}
			}
		}

		/**
		 * タスクの終了結果
		 *
		 */
		public class Result {
			ListenerTask task;
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
			map.put(Message.INIT_ONLINE_APP, new InitOnlineApp(this));
			map.put(Message.INIT_PHONE_BILL, new InitPhoneBill(this));
			map.put(Message.INIT_COMMAND_LINE_CLIENT, new InitCommandLineClient(this));
			map.put(Message.POLLING, new Polling(this));
			map.put(Message.GET_NEW_BLOCK, new GetNewBlock(this));
			map.put(Message.SUBMIT_BLOCK, new SubmitBlock(this));
			map.put(Message.GET_ACTIVE_BLOCK_INFO, new GetActiveBlockInfo(this));
		}

		@Override
		public void run() {
			int num = threadNumber.getAndAdd(1);
			Thread.currentThread().setName("ServerTask-" + num);
			try(NetworkIO io = new NetworkIO(socket)) {
				for(;;) {
					WholeMessage received = io.recieveFromClient();
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
					if (clientInfo.getStatus().isEndStatus()) {
						LOG.info("Receive a message the client has finished, last status = {}", clientInfo.getStatus().name());
						break;
					}
				}
			} catch (IOException | RuntimeException e) {
				LOG.warn("IO error, aborting server task....", e);
			}
			if (clientInfo != null && clientInfo.getType() ==ClientType.PHNEBILL) {
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
			this.clientInfo = clientInfo;
			clientInfos.add(clientInfo);
		}

		public ClientInfo getClientInfo() {
			return clientInfo;
		}

		/**
		 * @return singleProcessContractBlockManager
		 */
		public SingleProcessContractBlockManager getSingleProcessContractBlockManager() {
			return singleProcessContractBlockManager;
		}
	}
}
