package com.example.nedo.multinode.client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.app.ExecutableCommand;
import com.example.nedo.app.billing.PhoneBill;
import com.example.nedo.multinode.NetworkIO;
import com.example.nedo.multinode.NetworkIO.Message;
import com.example.nedo.multinode.server.ClientInfo.Status;
import com.example.nedo.online.AbstractOnlineApp;
import com.example.nedo.testdata.ContractBlockInfoAccessor;

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
	public void execute(String hostname, int port) throws UnknownHostException, IOException, SQLException  {
		Socket socket = new Socket(hostname, port);
		try (NetworkIO io = new NetworkIO(socket)) {
			List<String> response = io.request(Message.INIT_ONLINE_APP);
			config = Config.getConfigFromSrtring(String.join("\n", response));

			// configに従いオンラインアプリのリストを作成する

			ContractBlockInfoAccessor accessor = new ContractBlockInfoAccessorClient(io);
			List<AbstractOnlineApp>list = PhoneBill.createOnlineApps(config, accessor);
			final ExecutorService service = list.isEmpty() ? null : Executors.newFixedThreadPool(list.size());
			io.updateStatus(Status.READY, "Waiting.");
			io.poll(Collections.singleton(Message.REQUEST_RUN));
			try {
				list.stream().forEach(task -> service.submit(task));
				io.updateStatus(Status.RUNNING, "Stareted.");
				io.poll(Collections.singleton(Message.REQUEST_STOP));
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


}
