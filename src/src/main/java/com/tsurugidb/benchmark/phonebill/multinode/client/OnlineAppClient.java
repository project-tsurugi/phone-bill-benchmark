package com.tsurugidb.benchmark.phonebill.multinode.client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.ExecutableCommand;
import com.tsurugidb.benchmark.phonebill.app.billing.PhoneBill;
import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO;
import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO.Message;
import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo.Status;
import com.tsurugidb.benchmark.phonebill.online.AbstractOnlineApp;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;

public class OnlineAppClient extends ExecutableCommand{
    private static final Logger LOG = LoggerFactory.getLogger(OnlineAppClient.class);
    private Config config = null;


	public static void main(String[] args) throws Exception {
		String hostname = args[0].replaceAll(":.*", "");
		int port = Integer.parseInt(args[0].replaceAll(".*:", ""));
		OnlineAppClient onlineAppClient = new OnlineAppClient();
		onlineAppClient.execute(hostname, port);
	}

	@Override
	public void execute(String hostname, int port) throws UnknownHostException, IOException {
		Socket socket = new Socket(hostname, port);
		try (NetworkIO io = new NetworkIO(socket)) {
			List<String> response = io.request(Message.INIT_ONLINE_APP);
			config = Config.getConfigFromSrtring(String.join("\n", response));

			// configに従いオンラインアプリのリストを作成する

			ContractBlockInfoAccessor accessor = new ContractBlockInfoAccessorClient(io);
			List<AbstractOnlineApp>list = PhoneBill.createOnlineApps(config, accessor);
			final ExecutorService service = list.isEmpty() ? null : Executors.newFixedThreadPool(list.size());
			io.updateStatus(Status.READY, "Waiting.");
			Message message = io.waitReceiveMessageFromServer(new HashSet<>(Arrays.asList(Message.REQUEST_STOP, Message.REQUEST_RUN)));
			if (message == Message.REQUEST_STOP) {
				io.updateStatus(Status.RUNNING, "Aborted before running.");
				return;
			}
			try {
				// オンラインアプリを開始
				list.stream().forEach(task -> service.submit(task));
				io.updateStatus(Status.RUNNING, "Stareted.");
				Instant start = Instant.now();


				// サーバに処理状況を送りながらサーバからの終了指示まつ
				while (io.poll(Collections.singleton(Message.REQUEST_STOP)) == Message.REQUEST_NONE) {
					String status = createStatusMessage(start, Instant.now(), list);
					io.updateStatus(Status.RUNNING, status);
					io.sleepForPollingInterval();
				}

				// オンラインアプリを終了する
				list.stream().forEach(task -> task.terminate());
				if (service != null) {
					service.shutdown();
					service.awaitTermination(5, TimeUnit.MINUTES);
				}
				io.updateStatus(Status.SUCCESS, "Finished successfully.");
				LOG.info("Online application client finished successfully.");
			} catch (RuntimeException | InterruptedException e) {
				// System.exit()での終了を補足できないので改善する
				io.updateStatus(Status.FAIL, "Aborted with exception: " + e.getMessage());
				LOG.error("Online application client finished with an exception.", e);
			}
		}
	}

	/**
	 * 現在のステータスを表す文字列を作成する
	 *
	 * @param start
	 * @param now
	 * @param list
	 * @return
	 */
	static String createStatusMessage(Instant start, Instant now, List<AbstractOnlineApp> list) {
		double uptime = Duration.between(start, now).toMillis() / 1000d;
		Map<String, Integer> counterMap = list.stream().collect(
				Collectors.toMap(AbstractOnlineApp::getBaseName, AbstractOnlineApp::getExecCount, (c1, c2) -> c1 + c2));
		String counts = counterMap.entrySet().stream().map(e -> e.getKey() + " = " + e.getValue()).sorted()
				.collect(Collectors.joining(", "));
		return "uptime = " + uptime + " sec, exec count(" + counts + ")";
	}
}
