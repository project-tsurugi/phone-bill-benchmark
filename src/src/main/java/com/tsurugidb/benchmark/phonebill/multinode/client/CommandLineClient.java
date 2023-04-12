package com.tsurugidb.benchmark.phonebill.multinode.client;

import java.net.Socket;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.app.ExecutableCommand;
import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO;
import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO.Message;

/**
 * コマンドラインクライアント
 */
public class CommandLineClient extends ExecutableCommand {
	/**
	 * サーバに送信するメッセージ
	 */
	private Message message;

	/**
	 * コンストラクタ
	 *
	 * @param message サーバに送信するメッセージ
	 */
	public CommandLineClient(Message message) {
		this.message = message;
	}

	public static void main(String[] args) throws Exception {
		String hostname = args[0].replaceAll(":.*", "");
		int port = Integer.parseInt(args[0].replaceAll(".*:", ""));
		CommandLineClient client = new CommandLineClient(Message.valueOf(args[1]));
		client.execute(hostname, port);
	}

	@Override
	public void execute(String hostname, int port) throws Exception {
		Socket socket = new Socket(hostname, port);
		try (NetworkIO io = new NetworkIO(socket)) {
			List<String> response = io.request(message);
			for (String line : response) {
				System.out.println(line);
			}
		}
	}
}
