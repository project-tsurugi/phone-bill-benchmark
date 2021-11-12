package com.example.nedo.multinode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.multinode.server.ClientInfo.Status;

/**
 * ソケットを用いた通信のためのクラス
 * <p>
 * クライアントは複数のスレッドがこのインスタンスを共有しても正しく動作するように、
 * クライアントから使用するメソッドはsynchronizedによる同期を行っている。
 * サーバでは、単一スレッドがこのインスタンスを使用することを前提に同期処理を
 * していない。
 *
 */
public class NetworkIO implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkIO.class);

	private static final long POLLING_INTERVAL_MILLIS = 5000;
	private static final String END_OF_MESSAGE = "=== end of message ===";

	private Socket socket;
	private BufferedReader reader;
	private BufferedWriter writer;


	public NetworkIO(Socket socket) throws IOException {
		this.socket = socket;
		reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
		writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
	}

	/**
	 * ポーリングインターバルの間、スリープする
	 */
	public void sleepForPollingInterval() {
		try {
			Thread.sleep(POLLING_INTERVAL_MILLIS);
		} catch (InterruptedException e) {
			// nothing to do
		}
	}


	/**
	 * 指定のリクエストをサーバに送信し、サーバからのレスポンスの文字列を返す
	 *
	 * @param msg
	 * @return
	 * @throws IOException
	 */
	public List<String> request(Message msg) throws IOException {
		return request(msg, null);
	}

	/**
	 * 指定のメッセージボディ付きのリクエストをサーバに送信し、サーバからのレスポンスの文字列を返す
	 *
	 * @param msg
	 * @param msgBody
	 * @return
	 * @throws IOException
	 */
	public synchronized List<String> request(Message msg, String msgBody) throws IOException {
		if (msg.type != Type.REQUEST) {
			throw new IllegalArgumentException();
		}
		writeMessage(msg, msgBody);
		return readMessageBody();
	}


	/**
	 * サーバにメッセージを通知する
	 *
	 * @param msg
	 * @throws IOException
	 */
	public synchronized void notifyMessage(Message msg) throws IOException {
		if (msg.type != Type.NOTIFICATION) {
			throw new IllegalArgumentException();
		}
		writeMessage(msg, null);
	}

	/**
	 * @throws IOException
	 */
	private List<String> readMessageBody() throws IOException {
		List<String> list = new ArrayList<String>();
		for(;;) {
			String line = readLine();
			if (line.equals(END_OF_MESSAGE)) {
				return list;
			}
			list.add(line);
		}
	}

	/**
	 * @return
	 * @throws IOException
	 */
	private String readLine() throws IOException {
		String line = reader.readLine();
		if (line == null) {
			throw new IOException("Socket was closed,");
		}
		LOG.info("read line: {}", line);
		return line;
	}


	/**
	 * @param msgBody
	 * @throws IOException
	 */
	private void writeMessage(Message msg, String msgBody) throws IOException {
		writer.write(msg.name());
		writer.newLine();
		writeMessageBody(msgBody);
	}

	/**
	 * @param msgBody
	 * @throws IOException
	 */
	private void writeMessageBody(String msgBody) throws IOException {
		if (msgBody != null) {
			writer.write(msgBody);
			writer.newLine();
		}
		writer.write(END_OF_MESSAGE);
		writer.newLine();
		writer.flush();
	}

	/**
	 * サーバに対してポーリングする
	 *
	 * @param status クライアントのステータス
	 * @param clientMessage サーバに渡すクライアントの状態を表す文字列
	 * @return サーバからのレスポンス
	 * @throws IOException
	 */
	public synchronized Message poll(Status status, String clientMessage) throws IOException {
		String msgBody = status.name() + "\n" + clientMessage;
		writeMessage(Message.POLLING, msgBody);
		Message msg = readMessage();
		checkPollingResponse(msg);
		List<String> body = readMessageBody();
		if (!body.isEmpty()) {
			throw new IOException("Unexpected message body: " + String.join("\n", body));
		}
		return msg;
	}

	/**
	 * @return
	 * @throws IOException
	 */
	private Message readMessage() throws IOException {
		Message msg;
		try {
			String line = readLine();
			msg = Message.valueOf(line);
		} catch (IllegalArgumentException e) {
			throw new IOException("Protocol error", e);
		}
		return msg;
	}

	/**
	 * メッセージがポーリングのレスポンスかどうかをチェックする
	 *
	 * @param msg
	 * @throws IOException
	 */
	private void checkPollingResponse(Message msg) throws IOException {
		if (msg.type !=  Type.POLLING_RESPONSE) {
			throw new IOException("Protocol error, invalid message type: " + msg.name());
		}
	}


	/**
	 * クライアントからのポーリングに応答する
	 * @throws IOException
	 */
	public void response(Message msg) throws IOException {
		checkPollingResponse(msg);
		writeMessage(msg, null);
	}

	/**
	 * クライアントからのリクエストに応答する
	 */
	public void response(String msgBody) throws IOException {
		writeMessageBody(msgBody);
	}


	/**
	 * クライアントからのメッセージを受信する
	 *
	 * @return
	 * @throws IOException
	 */
	public WholeMessage recieveFromClient() throws IOException {
		WholeMessage ret = new WholeMessage();
		Message msg = readMessage();
		if (msg.type == Type.POLLING_RESPONSE) {
			throw new IOException("Protocol error, invalid message type: " + msg.name());
		}
		ret.message = msg;
		ret.body = readMessageBody();
		return ret;
	}



	/**
	 * メッセージを定義する列挙型、この文字列表現をメッセージヘッダとして用いる
	 */
	public enum Message {
		/**
		 * オンラインアプリクライアント接続.
		 * <p>
		 * サーバはコンフィグ情報を返す
		 */
		INIT_ONLINE_APP(Type.REQUEST),

		/**
		 * 料金計算バッチクライアントの初期化.
		 * <p>
		 * サーバはコンフィグ情報を返す
		 */
		INIT_PHONE_BILL(Type.REQUEST),

		/**
		 * コマンドラインクライアントの初期化
		 * <p>
		 * サーバはメッセージボディに従い処理をする
		 */
		INIT_COMMAND_LINE_CLIENT(Type.REQUEST),

		/**
		 * クライアントからポーリング
		 * <p>
		 * クライアントはクライアントのステータスを通知し、サーバはクライアントに対するリクエストがある場合
		 * レスポンスのメッセージボディにクライアントに対するリクエストセットしてレスポンスを返す
		 */
		POLLING(Type.POLLING),

		// クライアントに対するリクエスト

		/**
		 * プロセス停止
		 */
		REQUEST_SHUTDOWN(Type.POLLING_RESPONSE),

		/**
		 * バッチまたはオンラインアプリの停止
		 */
		REQUEST_STOP(Type.POLLING_RESPONSE),

		/**
		 * バッチまたはオンラインアプリの実行
		 */
		REQUEST_RUN(Type.POLLING_RESPONSE),


		/**
		 * クライアントからのポーリングに対するリクエストがない場合
		 */
		REQUEST_NONE(Type.POLLING_RESPONSE),


		// 契約マスタのブロック情報に関するコマンド

		/**
		 * 契約を生成するためのブロックのブロック番号を要求
		 */
		GET_NEW_BLOCK(Type.REQUEST),

		/**
		 * 指定されたブロック番号のブロックをアクティブなブロックとして通知する、
		 */
		SUBMIT_BLOCK(Type.NOTIFICATION),

		/**
		 * アクティブなブロックの情報を要求
		 */
		GET_ACTIVE_BLOCK_INFO(Type.REQUEST),

		// その他のメッセージ

		;


		/**
		 * メッセージ種別
		 */
		private Type type;

		/**
		 * メッセージの終端を表す文字列
		 */


		/**
		 * メッセージ種別を指定するためのコンストラクタ
		 *
		 * @param type
		 */
		private Message(Type type) {
			this.type = type;
		}

		/**
		 * @return type
		 */
		public final Type getType() {
			return type;
		}


	}

	/**
	 * メッセージの種別
	 */
	public enum Type {
		NOTIFICATION, // メッセージを送りリプライを待たないメッセージ
		REQUEST, // メッセージを送りリプライを待つメッセージ
		POLLING, // クライアントからのポーリング
		POLLING_RESPONSE, // クライアントからのポーリングに対してサーバが返すメッセージ
	}

	/**
	 * メッセージ全体を表すクラス.
	 * <p>
	 * メッセージの種別を表す列挙型とメッセージ本体を表す文字列で構成される
	 */
	public static class WholeMessage {
		/**
		 * メッセージの種別
		 */
		public Message message = null;
		/**
		 * メッセージ本体
		 */
		public List<String> body = null;
	}

	@Override
	public void close() throws IOException {
		socket.close();
	}
}
