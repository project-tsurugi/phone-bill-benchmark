package com.tsurugidb.benchmark.phonebill.multinode.client;

import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.ExecutableCommand;
import com.tsurugidb.benchmark.phonebill.app.billing.PhoneBill;
import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO;
import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO.Message;
import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo.Status;

public class PhoneBillClient extends ExecutableCommand{
    private static final Logger LOG = LoggerFactory.getLogger(PhoneBillClient.class);
    private Config config = null;


	public static void main(String[] args) throws Exception {
		String hostname = args[0].replaceAll(":.*", "");
		int port = Integer.parseInt(args[0].replaceAll(".*:", ""));
		PhoneBillClient phoneBillClient = new PhoneBillClient();
		phoneBillClient.execute(hostname, port);
	}

	@Override
	public void execute(String hostname, int port) throws Exception {
		Socket socket = new Socket(hostname, port);
		ExecutorService service = Executors.newFixedThreadPool(1);
		try (NetworkIO io = new NetworkIO(socket)) {
			List<String> response = io.request(Message.INIT_PHONE_BILL);
			config = Config.getConfigFromSrtring(String.join("\n", response));
			// オンラインアプリを動かさないようにconfigを修正する
			config.historyInsertThreadCount = 0;
			config.historyUpdateThreadCount = 0;
			config.masterDeleteInsertThreadCount = 0;
			config.masterUpdateThreadCount = 0;

			io.updateStatus(Status.READY, "Waiting.");
			Message message = io.waitReceiveMessageFromServer(new HashSet<>(Arrays.asList(Message.REQUEST_STOP, Message.REQUEST_RUN)));
			if (message == Message.REQUEST_STOP) {
				io.updateStatus(Status.RUNNING, "Aborted before running.");
				return;
			}
			io.updateStatus(Status.RUNNING, "Initializing.");

			// 別スレッドでバッチを実行
			PhoneBillTask task = new PhoneBillTask();
			Future<PhoneBillTask> future = service.submit(task);

			// サーバに進捗状況を通知しながらタスクの終了を待つ
			String prev = "";
			while (!future.isDone()) {
				message = io.poll(Collections.singleton(Message.REQUEST_STOP));
				if (message == Message.REQUEST_STOP) {
					task.abort();
				}
				io.sleepForPollingInterval();
				String status = task.getStatus();
				if (!prev.equals(status)) {
					io.updateStatus(Status.RUNNING, status);
					prev = status;
				}
			}
			// タスクの終了処理
			for(;;) {
				try {
					future.get();
					break; // タスクが正常終了
				} catch (InterruptedException e) {
					continue;
				} catch (ExecutionException e) {
					// System.exit()での終了を補足できないので改善する
					io.updateStatus(Status.FAIL, "Aborted with exception: " + e.getMessage());
					LOG.error("Phone bill client finished with an exception.", e);
					throw e;
				}
			}
			io.updateStatus(Status.SUCCESS, task.getFinalMessage());
			LOG.info("Phone bill client finished successfully.");
		} finally {
			service.shutdown();
			service.awaitTermination(5, TimeUnit.MINUTES);
		}
	}

	private class PhoneBillTask implements Callable<PhoneBillTask> {
		private PhoneBill phoneBill = new PhoneBill();

		@Override
		public PhoneBillTask call() throws Exception {
			phoneBill.execute(config);
			return this;
		}

		public void abort() {
			phoneBill.abort();
		}

		public String getStatus() {
			return phoneBill.getStatus();
		}

		public String getFinalMessage() {
			return phoneBill.getFinalMessage();
		}

	}
}
