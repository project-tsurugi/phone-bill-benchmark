package com.example.nedo.multinode.client;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
		OnlineAppClient phoneBillClient = new OnlineAppClient();
		phoneBillClient.execute(hostname, port);
	}

	@Override
	public void execute(String hostname, int port) throws Exception {
		Socket socket = new Socket(hostname, port);
		try (NetworkIO io = new NetworkIO(socket)) {
			List<String> response = io.request(Message.INIT_ONLINE_APP);
			config = Config.getConfigFromSrtring(String.join("\n", response));

			// configに従いオンラインアプリのリストを作成する

			ContractBlockInfoAccessor accessor = new ContractBlockInfoAccessorClient(io);
			List<AbstractOnlineApp>list = PhoneBill.createOnlineApps(config, accessor);
			final ExecutorService service = list.isEmpty() ? null : Executors.newFixedThreadPool(list.size());


			for(;;) {
				Message msg = io.poll(Status.READY, "Waiting for execute.");
				if (msg == Message.REQUEST_RUN) {
					break; // ループから抜けて実行開始
				}
				if (msg != Message.REQUEST_NONE) {
					throw new IOException("Protocol error, invalid message recived: " + msg.name());
				}
				io.sleepForPollingInterval();
			}
			try {
				list.parallelStream().forEach(task -> service.submit(task));
				for(;;) {
					io.sleepForPollingInterval();
				}
//				LOG.info("Phone bill client finushed successfully.");
			} catch (Exception e) {
				// System.exit()での終了を補足できないので改善する
				io.poll(Status.FAIL, "PhoneBill aborted with exception: " + e.getMessage());
				LOG.error("Phone bill client finushed with a exception.", e);
			}
		}
	}


}
