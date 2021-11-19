package com.example.nedo.multinode.client;

import java.net.Socket;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.app.ExecutableCommand;
import com.example.nedo.app.billing.PhoneBill;
import com.example.nedo.multinode.NetworkIO;
import com.example.nedo.multinode.NetworkIO.Message;
import com.example.nedo.multinode.server.ClientInfo.Status;

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
		try (NetworkIO io = new NetworkIO(socket)) {
			List<String> response = io.request(Message.INIT_PHONE_BILL);
			config = Config.getConfigFromSrtring(String.join("\n", response));
			// オンラインアプリを動かさないようにconfigを修正する
			config.historyInsertThreadCount = 0;
			config.historyUpdateThreadCount = 0;
			config.masterInsertThreadCount = 0;
			config.masterUpdateThreadCount = 0;

			PhoneBill phoneBill = new PhoneBill();

			io.updateStatus(Status.READY, "Waiting.");
			io.poll(Collections.singleton(Message.REQUEST_RUN));
			io.updateStatus(Status.RUNNING, "Stareted.");
			try {
				phoneBill.execute(config);
				io.updateStatus(Status.SUCCESS, "Finished successfully.");
				LOG.info("Phone bill client finished successfully.");
			} catch (Exception e) {
				// System.exit()での終了を補足できないので改善する
				io.updateStatus(Status.FAIL, "Aborted with exception: " + e.getMessage());
				LOG.error("Phone bill client finished with an exception.", e);
			}
		}
	}


}
