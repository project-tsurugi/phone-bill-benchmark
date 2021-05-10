package com.example.nedo.app;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.db.DBUtils;

public class Config implements Cloneable {

	private Properties prop;

	/* 料金計算に関するパラメータ */

	/**
	 * 計算対象日(指定の日を含む月を計算対象とする)
	 */
	public Date targetMonth;
	private static final String TARGET_MONTH = "target.month";

	/* 契約マスタ生成に関するパラメータ */

	/**
	 * 契約マスタのレコード数
	 */
	public int numberOfContractsRecords;
	private static final String NUMBER_OF_CONTRACTS_RECORDS = "number.of.contracts.records";

	/**
	 *  契約マスタの電話番号が重複する割合
	 */
	public int duplicatePhoneNumberRatio;
	private static final String DUPLICATE_PHONE_NUMBER_RATIO = "duplicate.phone.number.ratio";

	/**
	 * 契約終了日がある電話番号の割合
	 */
	public int expirationDateRate;
	private static final String EXPIRATION_DATE_RATE = "expiration.date.rate";

	/**
	 * 契約終了日がない電話番号の割合
	 */
	public int noExpirationDateRate;
	private static final String NO_EXPIRATION_DATE_RATE = "no.expiration.date.rate";

	/**
	 * 契約開始日の最小値
	 */
	public Date minDate;
	private static final String MIN_DATE = "min.date";

	/**
	 * 契約終了日の最大値
	 */
	public Date maxDate;
	private static final String MAX_DATE = "max.date";

	/* 通話履歴生成に関するパラメータ */

	/**
	 * 通話履歴のレコード数
	 */
	private static final String NUMBER_OF_HISTORY_RECORDS = "number.of.history.records";
	public int numberOfHistoryRecords;

	/* オンラインアプリケーションに関するパラメータ */

	/**
	 * 1分間に更新するマスタレコード数
	 */
	private static final String MASTER_UPDATE_RECORDS_PER_MIN = "master.update.records.per.min";
	public int masterUpdateRecordsPerMin;

	/**
	 * 1分間に追加するレコード数
	 */
	private static final String MASTER_INSERT_RECCRDS_PER_MIN = "master.insert.reccrds.per.min";
	public int masterInsertReccrdsPerMin;

	/**
	 * 1分間に追加する通話履歴レコード数
	 */
	private static final String HISTORY_UPDATE_RECORDS_PER_MIN = "history.update.records.per.min";
	public int historyUpdateRecordsPerMin;

	/**
	 * 1分間に発生する通話履歴インサートのトランザクション数
	 */
	private static final String HISTORY_INSERT_TRANSACTION_PER_MIN = "history.insert.transaction.per.min";
	public int historyInsertTransactionPerMin;

	/**
	 * 一回の通話履歴インサートのトランザックションで、インサートするレコード数
	 */
	private static final String HISTORY_INSERT_RECORDS_PER_TRANSACTION = "history.insert.records.per.transaction";
	public int historyInsertRecordsPerTransaction;

	/* jdbcのパラメータ */
	public String url;
	public String user;
	public String password;
	public int isolationLevel;
	private static final String URL = "url";
	private static final String USER = "user";
	private static final String PASSWORD = "password";
	private static final String THREAD_COUNT = "thread.count";
	private static final String ISOLATION_LEVEL = "isolation.level";
	private static final String STR_SERIALIZABLE = "SERIALIZABLE";
	private static final String STR_READ_COMMITTED = "READ_COMMITTED";

	/* スレッドに関するパラメータ */

	/**
	 * 料金計算スレッドのスレッド数
	 */
	public int threadCount;

	/**
	 * 料金計算のスレッドが、メインスレッドとJDBC Connectionを共有することを示すフラグ
	 */
	public boolean sharedConnection;
	private static final String SHARED_CONNECTION = "shared.connection";

	/* その他のパラメータ */

	/**
	 * 乱数のシード
	 */
	public long randomSeed;
	private static final String RANDOM_SEED = "random.seed";

	/**
	 * トランザクションのスコープ
	 */
	public TransactionScope transactionScope;
	private static final String TRANSACTION_SCOPE = "transaction.scope";

	/**
	 * コンストラクタ
	 *
	 * @param configFileName
	 * @throws IOException
	 */
	private Config(String configFileName) throws IOException {
		prop = new Properties();
		if (configFileName != null) {
			prop.load(Files.newBufferedReader(Paths.get(configFileName), StandardCharsets.UTF_8));
		}
		init();

		Logger logger = LoggerFactory.getLogger(Config.class);
		logger.info("Config initialized" +
				System.lineSeparator() + "--- " + System.lineSeparator() + this.toString() + "---");
	}

	/**
	 * config値を初期化する
	 */
	private void init() {
		// 料金計算に関するパラメータ
		targetMonth = getDate(TARGET_MONTH, DBUtils.toDate("2020-12-01"));

		// 契約マスタ生成に関するパラメータ
		numberOfContractsRecords = getInt(NUMBER_OF_CONTRACTS_RECORDS, 1000);
		duplicatePhoneNumberRatio = getInt(DUPLICATE_PHONE_NUMBER_RATIO, 10);
		expirationDateRate = getInt(EXPIRATION_DATE_RATE, 30);
		noExpirationDateRate = getInt(NO_EXPIRATION_DATE_RATE, 50);
		minDate = getDate(MIN_DATE, DBUtils.toDate("2010-11-11"));
		maxDate = getDate(MAX_DATE, DBUtils.toDate("2021-03-01"));

		// 通話履歴生成に関するパラメータ
		numberOfHistoryRecords = getInt(NUMBER_OF_HISTORY_RECORDS, 1000);

		// JDBCに関するパラメータ
		url = getString(URL, "jdbc:postgresql://127.0.0.1/phonebill");
		//		 url = getString(URL, "jdbc:oracle:thin:@localhost:1521:ORCL");
		user = getString(USER, "phonebill");
		password = getString(PASSWORD, "phonebill");
		isolationLevel = getIsolationLevel(ISOLATION_LEVEL, Connection.TRANSACTION_READ_COMMITTED);

		// スレッドに関するパラメータ
		threadCount = getInt(THREAD_COUNT, 1);
		sharedConnection = getBoolean(SHARED_CONNECTION, true);

		// オンラインアプリケーションに関するパラメータ
		masterUpdateRecordsPerMin = getInt(MASTER_UPDATE_RECORDS_PER_MIN, 0);
		masterInsertReccrdsPerMin = getInt(MASTER_INSERT_RECCRDS_PER_MIN, 0);
		historyUpdateRecordsPerMin = getInt(HISTORY_UPDATE_RECORDS_PER_MIN, 0);
		historyInsertTransactionPerMin = getInt(HISTORY_INSERT_TRANSACTION_PER_MIN, 0);
		historyInsertRecordsPerTransaction = getInt(HISTORY_INSERT_RECORDS_PER_TRANSACTION, 1);

		// その他のパラメータ
		randomSeed = getLong(RANDOM_SEED, 0);
		transactionScope = gettransactionScope(TRANSACTION_SCOPE, TransactionScope.WHOLE);

		// パラメータ間の矛盾のチェック
		if (transactionScope == TransactionScope.CONTRACT && sharedConnection) {
			// トランザクションのスコープが契約単位で、コネクション共有は許されない
			throw new RuntimeException("TransactionScope Contract and sharedConnection cannot be specified at the same time.");
		}

	}

	/**
	 * トランザックションスコープを取得する
	 *
	 * @param transactionScope
	 * @param whole
	 * @return
	 */
	private TransactionScope gettransactionScope(String key, TransactionScope defaultValue) {
		if (!prop.containsKey(key)) {
			return defaultValue;
		}

		if (prop.getProperty(key).equalsIgnoreCase(TransactionScope.CONTRACT.toString())) {
			return TransactionScope.CONTRACT;
		} else if (prop.getProperty(key).equalsIgnoreCase(TransactionScope.WHOLE.toString())) {
			return TransactionScope.WHOLE;
		} else {
			throw new RuntimeException("Unsupported transaction scope: " + prop.getProperty(key) + ", only '"
					+ TransactionScope.CONTRACT + "' or '" + TransactionScope.WHOLE + "' are supported.");
		}
	}

	/**
	 * Transaction Isolation Levelを取得する
	 *
	 * @param string
	 * @param transactionSerializable
	 * @return
	 */
	private int getIsolationLevel(String key, int defaultValue) {
		if (!prop.containsKey(key)) {
			return defaultValue;
		}
		switch (prop.getProperty(key)) {
		case STR_READ_COMMITTED:
			return Connection.TRANSACTION_READ_COMMITTED;
		case STR_SERIALIZABLE:
			return Connection.TRANSACTION_SERIALIZABLE;
		default:
			throw new RuntimeException("Unsupported transaction isolation level: "
					+ prop.getProperty(key) + ", only '" + STR_READ_COMMITTED + "' or '" + STR_SERIALIZABLE
					+ "' are supported.");
		}
	}

	private static String toIsolationLevelString(int isolationLevel) {
		switch (isolationLevel) {
		case Connection.TRANSACTION_SERIALIZABLE:
			return STR_SERIALIZABLE;
		case Connection.TRANSACTION_READ_COMMITTED:
			return STR_READ_COMMITTED;
		default:
			return "Unspoorted Isolation Level";
		}
	}

	/**
	 * int型のプロパティの値を取得する
	 *
	 * @param key プロパティ名
	 * @param defaultValue プロパティが存在しない時のデフォルト値
	 * @return
	 */
	private int getInt(String key, int defaultValue) {
		int value = defaultValue;
		if (prop.containsKey(key)) {
			String s = prop.getProperty(key);
			value = Integer.parseInt(s);
		}
		return value;
	}

	/**
	 * long型のプロパティの値を取得する
	 *
	 * @param key プロパティ名
	 * @param defaultValue プロパティが存在しない時のデフォルト値
	 * @return
	 */
	private long getLong(String key, long defaultValue) {
		long value = defaultValue;
		if (prop.containsKey(key)) {
			String s = prop.getProperty(key);
			value = Long.parseLong(s);
		}
		return value;
	}

	/**
	 * boolean型のプロパティの値を取得する
	 *
	 * @param key プロパティ名
	 * @param defaultValue プロパティが存在しない時のデフォルト値
	 * @return
	 */
	private boolean getBoolean(String key, boolean defaultValue) {
		if (prop.containsKey(key)) {
			String value = prop.getProperty(key);
			return toBoolan(value);
		}
		return defaultValue;
	}

	/**
	 * 文字列をbooleanに変換する
	 *
	 * @param value
	 * @return
	 */
	static boolean toBoolan(String value) {
		String s = value.trim().toLowerCase();
		switch (s) {
		case "yes":
			return true;
		case "true":
			return true;
		case "1":
			return true;
		case "no":
			return false;
		case "false":
			return false;
		case "0":
			return false;
		default:
			throw new RuntimeException("Illegal property value: " + value);
		}
	}

	/**
	 * Stringのプロパティの値を取得する
	 *
	 * @param key プロパティ名
	 * @param defaultValue プロパティが存在しない時のデフォルト値
	 * @return
	 */
	private String getString(String key, String defaultValue) {
		String value = defaultValue;
		if (prop.containsKey(key)) {
			value = prop.getProperty(key);
		}
		return value;
	}

	/**
	 * Date型のプロパティの値を取得する
	 *
	 * @param key プロパティ名
	 * @param defaultValue プロパティが存在しない時のデフォルト値
	 * @return
	 */
	private Date getDate(String key, Date defaultValue) {
		Date value = defaultValue;
		if (prop.containsKey(key)) {
			String s = prop.getProperty(key);
			value = DBUtils.toDate(s);
		}
		return value;
	}

	/**
	 * configオブジェクトの生成.
	 * <br>
	 * コマンドライン引数で指定されたファイル名のファイルを設定ファイルとみなして
	 * configオブジェクトを生成する。引数が指定されていない場合は、デフォルト値で
	 * configオブジェクトを生成する。
	 *
	 * @param args
	 * @return
	 * @throws IOException
	 */
	public static Config getConfig(String[] args) throws IOException {
		if (args.length == 0) {
			return new Config(null);
		}
		return new Config(args[0]);
	}

	public static Config getConfig() throws IOException {
		return getConfig(new String[0]);
	}

	@Override
	public String toString() {
		String format = "%s=%s%n";
		String commentFormat = "# %s%n";
		StringBuilder sb = new StringBuilder();

		sb.append(String.format(commentFormat, "料金計算に関するパラメータ"));
		sb.append(String.format(format, TARGET_MONTH, targetMonth));
		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "契約マスタ生成に関するパラメータ"));
		sb.append(String.format(format, NUMBER_OF_CONTRACTS_RECORDS, numberOfContractsRecords));
		sb.append(String.format(format, DUPLICATE_PHONE_NUMBER_RATIO, duplicatePhoneNumberRatio));
		sb.append(String.format(format, EXPIRATION_DATE_RATE, expirationDateRate));
		sb.append(String.format(format, NO_EXPIRATION_DATE_RATE, noExpirationDateRate));
		sb.append(String.format(format, MIN_DATE, minDate));
		sb.append(String.format(format, MAX_DATE, maxDate));
		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "通話履歴生成に関するパラメータ"));
		sb.append(String.format(format, NUMBER_OF_HISTORY_RECORDS, numberOfHistoryRecords));
		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "JDBCに関するパラメータ"));
		sb.append(String.format(format, URL, url));
		sb.append(String.format(format, USER, user));
		sb.append(String.format(format, PASSWORD, password));
		sb.append(String.format(format, ISOLATION_LEVEL, toIsolationLevelString(isolationLevel)));
		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "オンラインアプリケーションに関するパラメータ"));
		sb.append(String.format(format, MASTER_UPDATE_RECORDS_PER_MIN, masterUpdateRecordsPerMin));
		sb.append(String.format(format, MASTER_INSERT_RECCRDS_PER_MIN, masterInsertReccrdsPerMin));
		sb.append(String.format(format, HISTORY_UPDATE_RECORDS_PER_MIN, historyUpdateRecordsPerMin));
		sb.append(String.format(format, HISTORY_INSERT_TRANSACTION_PER_MIN, historyInsertTransactionPerMin));
		sb.append(String.format(format, HISTORY_INSERT_RECORDS_PER_TRANSACTION, historyInsertRecordsPerTransaction));
		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "スレッドに関するパラメータ"));
		sb.append(String.format(format, THREAD_COUNT, threadCount));
		sb.append(String.format(format, SHARED_CONNECTION, sharedConnection));
		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "その他のパラメータ"));
		sb.append(String.format(format, RANDOM_SEED, randomSeed));
		sb.append(String.format(format, TRANSACTION_SCOPE, transactionScope));
		return sb.toString();
	}

	public static enum TransactionScope {
		WHOLE, CONTRACT
	}

	@Override
	public Config clone()  {
		try {
			return (Config) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new InternalError(e);
		}
	}
}
