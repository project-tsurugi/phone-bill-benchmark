package com.tsurugidb.benchmark.phonebill.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.util.Locale;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.jdbc.DBUtils;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;
import com.tsurugidb.benchmark.phonebill.db.oracle.PhoneBillDbManagerOracle;
import com.tsurugidb.benchmark.phonebill.db.postgresql.PhoneBillDbManagerPostgresql;

public class Config implements Cloneable {
    private static final Logger LOG = LoggerFactory.getLogger(Config.class);

	/**
	 * プロパティ
	 */
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
	public int duplicatePhoneNumberRate;
	private static final String DUPLICATE_PHONE_NUMBER_RATIO = "duplicate.phone.number.rate";

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
	public long numberOfHistoryRecords;
	private static final String NUMBER_OF_HISTORY_RECORDS = "number.of.history.records";

	/**
	 * 発信電話番号の分布関数
	 */
	public DistributionFunction callerPhoneNumberDistribution;
	private static final String CALLER_PHONE_NUMBER_DISTRIBUTION = "caller.phone.number.distribution";

	/**
	 * 発信電話番号の分布関数に対数正規分布を指定したときに用いるscaleの値
	 */
	public double callerPhoneNumberScale;
	private static final String CALLER_PHONE_NUMBER_SCALE = "caller.phone.number.scale";

	/**
	 * 発信電話番号の分布関数に対数正規分布を指定したときに用いるshapeの値
	 */
	public double callerPhoneNumberShape;
	private static final String CALLER_PHONE_NUMBER_SHAPE = "caller.phone.number.shape";

	/**
	 * 受信電話番号の分布関数
	 */
	public DistributionFunction recipientPhoneNumberDistribution;
	private static final String RECIPIENT_PHONE_NUMBER_DISTRIBUTION = "recipient.phone.number.distribution";

	/**
	 * 受信電話番号の分布関数に対数正規分布を指定したときに用いるscaleの値
	 */
	public double recipientPhoneNumberScale;
	private static final String RECIPIENT_PHONE_NUMBER_SCALE = "recipient.phone.number.scale";

	/**
	 * 受信電話番号の分布関数に対数正規分布を指定したときに用いるshapeの値
	 */
	public double recipientPhoneNumberShape;
	private static final String RECIPIENT_PHONE_NUMBER_SHAPE = "recipient.phone.number.shape";

	/**
	 * 通話時間の分布関数
	 */
	public DistributionFunction callTimeDistribution;
	private static final String CALL_TIME_DISTRIBUTION = "call.time.distribution";

	/**
	 * 通話時間の分布関数に対数正規分布を指定したときに用いるscaleの値
	 */
	public double callTimeScale;
	private static final String CALL_TIME_SCALE = "call.time.scale";

	/**
	 * 通話時間の分布関数に対数正規分布を指定したときに用いるshapeの値
	 */
	public double callTimeShape;
	private static final String CALL_TIME_SHAPE = "call.time.shape";


	/**
	 * 通話時間の最大値(秒)
	 */
	public int maxCallTimeSecs;
	private static final String MAX_CALL_TIME_SECS = "max.call.time.secs";

	/**
	 * 統計情報を出力するディレクトリのパス
	 */
	public String statisticsOutputDir;
	private static final String STATISTICS_OUTPUT_DIR = "statistics.output.dir";

	/**
	 * 通話履歴の通話開始時刻の最小値
	 */
	public Date historyMinDate;
	private static final String HISTORY_MIN_DATE = "history.min.date";

	/**
	 * 通話履歴の通話開始時刻の最大値
	 */
	public Date historyMaxDate;
	private static final String HISTORY_MAX_DATE = "history.max.date";


	/* オンラインアプリケーションに関するパラメータ */

	/**
	 * 1分間に更新する1スレッドあたりのマスタのレコード数
	 */
	public int masterUpdateRecordsPerMin;
	private static final String MASTER_UPDATE_RECORDS_PER_MIN = "master.update.records.per.min";

	/**
	 * 1分間に追加する1スレッドあたりのマスタのレコード数
	 */
	public int masterInsertReccrdsPerMin;
	private static final String MASTER_INSERT_RECCRDS_PER_MIN = "master.insert.reccrds.per.min";

	/**
	 * 1分間に更新する1スレッドあたりの通話履歴レコード数
	 */
	public int historyUpdateRecordsPerMin;
	private static final String HISTORY_UPDATE_RECORDS_PER_MIN = "history.update.records.per.min";

	/**
	 * 1分間に発生する通話履歴追加の1スレッドあたりのトランザクション数
	 */
	public int historyInsertTransactionPerMin;
	private static final String HISTORY_INSERT_TRANSACTION_PER_MIN = "history.insert.transaction.per.min";


	/**
	 * マスタをアップデートするアプリのスレッド数
	 */
	public int masterUpdateThreadCount;
	private static final String MASTER_UPDATE_THREAD_COUNT = "master.update.thread.count";

	/**
	 * マスタを追加するアプリのスレッド数
	 */
	public int masterInsertThreadCount;
	private static final String 	MASTER_INSERT_THREAD_COUNT = "master.insert.thread.count";

	/**
	 * 履歴をアップデートするアプリのスレッド数
	 */
	public int historyUpdateThreadCount;
	private static final String HISTORY_UPDATE_THREAD_COUNT = "history.update.thread.count";

	/**
	 * 履歴を追加するアプリのスレッド数
	 */
	public int historyInsertThreadCount;
	private static final String HISTORY_INSERT_THREAD_COUNT = "history.insert.thread.count";


	/**
	 * 一回の通話履歴追加のトランザックションで、インサートするレコード数
	 */
	public int historyInsertRecordsPerTransaction;
	private static final String HISTORY_INSERT_RECORDS_PER_TRANSACTION = "history.insert.records.per.transaction";


	/**
	 * オンラインアプリケーションのDBアクセスをスキップする(オンラインアプリのエンジンの性能テスト用)
	 */
	public boolean skipDatabaseAccess;
	private static final String SKIP_DATABASE_ACCESS = "skip.database.access";


	/* jdbcのパラメータ */
	public String url;
	public String user;
	public String password;
	public IsolationLevel isolationLevel;
	private static final String URL = "url";
	private static final String USER = "user";
	private static final String PASSWORD = "password";
	private static final String ISOLATION_LEVEL = "isolation.level";


	/**
	 * DBMSタイプ
	 */
	public DbmsType dbmsType;
	private static final String DBMS_TYPE= "dbms.type";


	/* スレッドに関するパラメータ */

	/**
	 * 料金計算スレッドのスレッド数
	 */
	public int threadCount;
	private static final String THREAD_COUNT = "thread.count";

	/**
	 * 料金計算のスレッドが、メインスレッドとJDBC Connectionを共有することを示すフラグ
	 */
	public boolean sharedConnection;
	private static final String SHARED_CONNECTION = "shared.connection";


	/* CSVデータに関するパラメータ */
	public String csvDir;
	private static final String CSV_DIR = "csv.dir";


	/* 履歴のCSVファイル１ファイルの最大行数 */
	public int maxNumberOfLinesHistoryCsv;
	private static final String MAX_NUMBER_OF_LINES_HISTORY_CSV = "max.number.of.lines.history.csv";

	/* Oracle固有のパラメータ */

	/**
	 * テーブル生成時に指定するinitransの値
	 */
	public int oracleInitran;
	private static final String ORACLE_INITRAN = "oracle.initrans";


	/**
	 * SQL*Loaderのパス
	 */
	public String oracleSqlLoaderPath;
	private static final String ORACLE_SQL_LOADER_PATH =  "oracle.sql.loader.path";

	/**
	 * SQL*Loader実行時に指定するSID
	 */
	public String oracleSqlLoaderSid;
	private static final String ORACLE_SQL_LOADER_SID =  "oracle.sql.loader.sid";


	/**
	 * Index生成時に適用するオプション
	 */
	public String oracleCreateIndexOption;
	private static final String ORACLE_CREATE_INDEX_OPTION = "oracle.create.index.option";


	/* その他のパラメータ */

	/**
	 * 乱数のシード
	 */
	public int randomSeed;
	private static final String RANDOM_SEED = "random.seed";

	/**
	 * トランザクションのスコープ
	 */
	public TransactionScope transactionScope;
	private static final String TRANSACTION_SCOPE = "transaction.scope";

	/**
	 * システムプロパティ経由で設定する場合のプロパティキープレフィックス
	 */
	private static final String SYSPROP_PREFIX = "phone-bill.";

	/**
	 * 複数ノード構成時のサーバのリッスンポート
	 */
	public int listenPort;
	private static final String LISTEN_PORT = "listen.port";



	/**
	 * このconfigで使用するPhoneBillDbManagerのインスタンス
	 * TODO: UTつくる
	 */
	private PhoneBillDbManager dbManager;


	/**
	 * コンストラクタ.
	 * <br>
	 *
	 *
	 * @param reader
	 * @throws IOException
	 */
	private Config(Reader reader) throws IOException {
		init(reader);
	}


	/**
	 * 指定のファイル名のファイルを用いてインスタンスを初期化する
	 *
	 * @param finaname
	 * @throws IOException
	 */
	private Config(String configFileName) throws IOException {
		if (configFileName == null) {
			init(null);
		} else {
			try (BufferedReader br = Files.newBufferedReader(Paths.get(configFileName), StandardCharsets.UTF_8)) {
				init(br);
			}
		}
	}


	/**
	 * readerからコンフィグ値を読み取りインスタンスを初期化する。readerにnullが指定されたときはデフォルト値で初期化する
	 *
	 * @param reader
	 * @throws IOException
	 */
	private void init(Reader reader) throws IOException {
		prop = new Properties();
		if (reader != null) {
			prop.load(reader);
		}
		System.getProperties().stringPropertyNames().stream()
			.filter(k -> k.startsWith(SYSPROP_PREFIX))
			.forEach(k -> prop.put(k.substring(SYSPROP_PREFIX.length()), System.getProperty(k)));
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
		duplicatePhoneNumberRate = getInt(DUPLICATE_PHONE_NUMBER_RATIO, 10);
		expirationDateRate = getInt(EXPIRATION_DATE_RATE, 30);
		noExpirationDateRate = getInt(NO_EXPIRATION_DATE_RATE, 50);
		minDate = getDate(MIN_DATE, DBUtils.toDate("2010-11-11"));
		maxDate = getDate(MAX_DATE, DBUtils.toDate("2021-03-01"));

		// 通話履歴生成に関するパラメータ
		numberOfHistoryRecords = getLong(NUMBER_OF_HISTORY_RECORDS, 1000);
		callerPhoneNumberDistribution = getDistributionFunction(CALLER_PHONE_NUMBER_DISTRIBUTION, DistributionFunction.UNIFORM);
		callerPhoneNumberScale = getDouble(CALLER_PHONE_NUMBER_SCALE, 3);
		callerPhoneNumberShape = getDouble(CALLER_PHONE_NUMBER_SHAPE, 18);
		recipientPhoneNumberDistribution = getDistributionFunction(RECIPIENT_PHONE_NUMBER_DISTRIBUTION, DistributionFunction.UNIFORM);
		recipientPhoneNumberScale = getDouble(RECIPIENT_PHONE_NUMBER_SCALE, 3d);
		recipientPhoneNumberShape = getDouble(RECIPIENT_PHONE_NUMBER_SHAPE, 18d);
		callTimeDistribution = getDistributionFunction(CALL_TIME_DISTRIBUTION, DistributionFunction.UNIFORM);
		callTimeScale = getDouble(CALL_TIME_SCALE, 4.5d);
		callTimeShape = getDouble(CALL_TIME_SHAPE, 1.5d);
		maxCallTimeSecs = getInt(MAX_CALL_TIME_SECS, 3600);
		statisticsOutputDir  = getString(STATISTICS_OUTPUT_DIR, null);
		historyMinDate = getDate(HISTORY_MIN_DATE, DBUtils.toDate("2020-11-01"));
		historyMaxDate = getDate(HISTORY_MAX_DATE, DBUtils.toDate("2021-01-10"));

		// JDBCに関するパラメータ
		url = getString(URL, "jdbc:postgresql://127.0.0.1/phonebill");
		user = getString(USER, "phonebill");
		password = getString(PASSWORD, "phonebill");
		isolationLevel = getIsolationLevel(ISOLATION_LEVEL, IsolationLevel.READ_COMMITTED);

		// DBMSタイプ(明示的に指定されていない場合はURLから決定する)
		dbmsType = getDbmsType(DBMS_TYPE, url);


		// スレッドに関するパラメータ
		threadCount = getInt(THREAD_COUNT, 1);
		sharedConnection = getBoolean(SHARED_CONNECTION, true);

		// オンラインアプリケーションに関するパラメータ
		masterUpdateRecordsPerMin = getInt(MASTER_UPDATE_RECORDS_PER_MIN, 0);
		masterUpdateThreadCount = getInt(MASTER_UPDATE_THREAD_COUNT, 1);

		masterInsertReccrdsPerMin = getInt(MASTER_INSERT_RECCRDS_PER_MIN, 0);
		masterInsertThreadCount = getInt(MASTER_INSERT_THREAD_COUNT, 1);

		historyUpdateRecordsPerMin = getInt(HISTORY_UPDATE_RECORDS_PER_MIN, 0);
		historyUpdateThreadCount = getInt(HISTORY_UPDATE_THREAD_COUNT, 1);

		historyInsertTransactionPerMin = getInt(HISTORY_INSERT_TRANSACTION_PER_MIN, 0);
		historyInsertThreadCount = getInt(HISTORY_INSERT_THREAD_COUNT, 1);
		historyInsertRecordsPerTransaction = getInt(HISTORY_INSERT_RECORDS_PER_TRANSACTION, 1);

		skipDatabaseAccess = getBoolean(SKIP_DATABASE_ACCESS, false);

		//  CSVデータに関するパラメータ
		csvDir = getString(CSV_DIR, "/var/lib/csv");
		maxNumberOfLinesHistoryCsv = getInt(MAX_NUMBER_OF_LINES_HISTORY_CSV, 1000*1000);

		// Oracle固有のパラメータ
		oracleInitran = getInt(ORACLE_INITRAN, 0);
		oracleSqlLoaderPath = getString(ORACLE_SQL_LOADER_PATH, "sqlldr");
		oracleSqlLoaderSid = getString(ORACLE_SQL_LOADER_SID, "");
		oracleCreateIndexOption = getString(ORACLE_CREATE_INDEX_OPTION, "nologging parallel 32");

		// その他のパラメータ
		randomSeed = getInt(RANDOM_SEED, 0);
		transactionScope = getTransactionScope(TRANSACTION_SCOPE, TransactionScope.WHOLE);

		// パラメータ間の矛盾のチェック
		if (transactionScope == TransactionScope.CONTRACT && sharedConnection) {
			// トランザクションのスコープが契約単位で、コネクション共有は許されない
			throw new RuntimeException("TransactionScope Contract and sharedConnection cannot be specified at the same time.");
		}
		listenPort = getInt(LISTEN_PORT, 0);
	}

	/**
	 * トランザックションスコープを取得する
	 *
	 * @param key プロパティ名
	 * @param defaultValue プロパティが存在しない時のデフォルト値
	 * @return
	 */
	private TransactionScope getTransactionScope(String key, TransactionScope defaultValue) {
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
	 * 分布関数を取得する
	 *
	 * @param key プロパティ名
	 * @param defaultValue プロパティが存在しない時のデフォルト値
	 * @return
	 */
	private DistributionFunction getDistributionFunction(String key, DistributionFunction defaultValue) {
		if (!prop.containsKey(key)) {
			return defaultValue;
		}
		if (prop.getProperty(key).equalsIgnoreCase(DistributionFunction.UNIFORM.toString())) {
			return DistributionFunction.UNIFORM;
		} else if (prop.getProperty(key).equalsIgnoreCase(DistributionFunction.LOGNORMAL.toString())) {
			return DistributionFunction.LOGNORMAL;
		} else {
			throw new RuntimeException("Unknown distribution function type: " + prop.getProperty(key) + ", only '"
					+ DistributionFunction.UNIFORM + "' or '" + DistributionFunction.LOGNORMAL
					+ "' are supported.");
		}
	}


	/**
	 * Transaction Isolation Levelを取得する
	 *
	 * @param string
	 * @param transactionSerializable
	 * @return
	 */
	private IsolationLevel getIsolationLevel(String key, IsolationLevel defaultValue) {
		if (!prop.containsKey(key)) {
			return defaultValue;
		}
		return IsolationLevel.valueOf(prop.getProperty(key));
	}


	/**
	 * DBMSタイプを取得する
	 *
	 * @param key
	 * @param url
	 * @return
	 */
	private DbmsType getDbmsType(String key, String url) {
		if (!prop.containsKey(key)) {
			// 明示的に指定されていない場合はURLから決定する
			if (url.toLowerCase(Locale.JAPAN).contains("oracle")) {
				return DbmsType.ORACLE_JDBC;
			} else if (url.toLowerCase(Locale.JAPAN).contains("postgresql")) {
				return DbmsType.POSTGRE_SQL_JDBC;
			} else {
				return DbmsType.OTHER;
			}
		}

		String s = prop.getProperty(key);
		try {
			 return DbmsType.valueOf(s);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException("unkown " + DBMS_TYPE + ": " + s);
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
	 * double型のプロパティの値を取得する
	 *
	 * @param key プロパティ名
	 * @param defaultValue プロパティが存在しない時のデフォルト値
	 * @return
	 */
	private double getDouble(String key, double defaultValue) {
		if (prop.containsKey(key)) {
			String value = prop.getProperty(key);
			return Double.parseDouble(value);
		}
		return defaultValue;
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
		String s = value.trim().toLowerCase(Locale.JAPANESE);
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
		if (prop.containsKey(key)) {
			String value = prop.getProperty(key);
			if (value.length() == 0) {
				return defaultValue;
			}
			return value;
		}
		return defaultValue;
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
	public static Config getConfig(String arg) throws IOException {
		return new Config(arg);
	}

	/**
	 * configオブジェクトの生成.
	 * <br>
	 * デフォルト値でconfigオブジェクトを生成する。

	 * @return
	 * @throws IOException
	 */
	public static Config getConfig() throws IOException {
		return new Config((Reader)null);
	}


	/**
	 * 指定の文字列のconfigを作成する
	 *
	 * @param string
	 * @return
	 * @throws IOException
	 */
	public static Config getConfigFromSrtring(String string) throws IOException {
		StringReader reader = new StringReader(string);
		return new Config(reader);
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
		sb.append(String.format(format, DUPLICATE_PHONE_NUMBER_RATIO, duplicatePhoneNumberRate));
		sb.append(String.format(format, EXPIRATION_DATE_RATE, expirationDateRate));
		sb.append(String.format(format, NO_EXPIRATION_DATE_RATE, noExpirationDateRate));
		sb.append(String.format(format, MIN_DATE, minDate));
		sb.append(String.format(format, MAX_DATE, maxDate));
		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "通話履歴生成に関するパラメータ"));
		sb.append(String.format(format, NUMBER_OF_HISTORY_RECORDS, numberOfHistoryRecords));
		sb.append(String.format(format, RECIPIENT_PHONE_NUMBER_DISTRIBUTION, recipientPhoneNumberDistribution));
		sb.append(String.format(format, RECIPIENT_PHONE_NUMBER_SCALE, recipientPhoneNumberScale));
		sb.append(String.format(format, RECIPIENT_PHONE_NUMBER_SHAPE, recipientPhoneNumberShape));
		sb.append(String.format(format, CALLER_PHONE_NUMBER_DISTRIBUTION, callerPhoneNumberDistribution));
		sb.append(String.format(format, CALLER_PHONE_NUMBER_SCALE, callerPhoneNumberScale));
		sb.append(String.format(format, CALLER_PHONE_NUMBER_SHAPE, callerPhoneNumberShape));
		sb.append(String.format(format, CALL_TIME_DISTRIBUTION, callTimeDistribution));
		sb.append(String.format(format, CALL_TIME_SCALE, callTimeScale));
		sb.append(String.format(format, CALL_TIME_SHAPE, callTimeShape));
		sb.append(String.format(format, MAX_CALL_TIME_SECS, maxCallTimeSecs));
		sb.append(String.format(format, STATISTICS_OUTPUT_DIR, statisticsOutputDir == null ? "" : statisticsOutputDir));
		sb.append(String.format(format, HISTORY_MIN_DATE, historyMinDate));
		sb.append(String.format(format, HISTORY_MAX_DATE, historyMaxDate));
		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "JDBCに関するパラメータ"));
		sb.append(String.format(format, URL, url));
		sb.append(String.format(format, USER, user));
		sb.append(String.format(format, PASSWORD, password));
		sb.append(String.format(format, ISOLATION_LEVEL, isolationLevel));
		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "DBMSタイプ"));
		sb.append(String.format(format, DBMS_TYPE, dbmsType));
		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "オンラインアプリケーションに関するパラメータ"));
		sb.append(String.format(format, MASTER_UPDATE_RECORDS_PER_MIN, masterUpdateRecordsPerMin));
		sb.append(String.format(format, MASTER_UPDATE_THREAD_COUNT, masterUpdateThreadCount));
		sb.append(String.format(format, MASTER_INSERT_RECCRDS_PER_MIN, masterInsertReccrdsPerMin));
		sb.append(String.format(format, MASTER_INSERT_THREAD_COUNT, masterInsertThreadCount));
		sb.append(String.format(format, HISTORY_UPDATE_RECORDS_PER_MIN, historyUpdateRecordsPerMin));
		sb.append(String.format(format, HISTORY_UPDATE_THREAD_COUNT, historyUpdateThreadCount));
		sb.append(String.format(format, HISTORY_INSERT_TRANSACTION_PER_MIN, historyInsertTransactionPerMin));
		sb.append(String.format(format, HISTORY_INSERT_RECORDS_PER_TRANSACTION, historyInsertRecordsPerTransaction));
		sb.append(String.format(format, HISTORY_INSERT_THREAD_COUNT, historyInsertThreadCount));
		sb.append(String.format(format, SKIP_DATABASE_ACCESS, skipDatabaseAccess));
		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "スレッドに関するパラメータ"));
		sb.append(String.format(format, THREAD_COUNT, threadCount));
		sb.append(String.format(format, SHARED_CONNECTION, sharedConnection));
		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "CSVに関するパラメータ"));
		sb.append(String.format(format, CSV_DIR, csvDir));
		sb.append(String.format(format, MAX_NUMBER_OF_LINES_HISTORY_CSV, maxNumberOfLinesHistoryCsv));

		sb.append(System.lineSeparator());
		sb.append(String.format(commentFormat, "Oracle固有のパラメータ"));
		sb.append(String.format(format, ORACLE_INITRAN, oracleInitran));
		sb.append(String.format(format, ORACLE_SQL_LOADER_PATH, oracleSqlLoaderPath));
		sb.append(String.format(format, ORACLE_SQL_LOADER_SID, oracleSqlLoaderSid));
		sb.append(String.format(format, ORACLE_CREATE_INDEX_OPTION, oracleCreateIndexOption));
		sb.append(String.format(commentFormat, "その他のパラメータ"));
		sb.append(String.format(format, RANDOM_SEED, randomSeed));
		sb.append(String.format(format, TRANSACTION_SCOPE, transactionScope));
		sb.append(String.format(format, LISTEN_PORT, listenPort));
		return sb.toString();
	}

	/**
	 * トランザクションスコープ
	 *
	 */
	public static enum TransactionScope {
		/**
		 * バッチ全体をトランザクションとする
		 */
		WHOLE,

		/**
		 * 1契約の処理を1トランザクションとする
		 */
		CONTRACT
	}

	/**
	 * テストデータ生成時に使用する分布関数
	 *
	 */
	public static enum DistributionFunction {
		UNIFORM, // 一様分布
		LOGNORMAL, // 対数正規分布
	}


	/**
	 * 使用するDBMSのタイプ
	 *
	 */
	public static enum DbmsType {
		ORACLE_JDBC,
		POSTGRE_SQL_JDBC,
		OTHER,
	}

	@Override
	public Config clone()  {
		try {
			return (Config) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new InternalError(e);
		}
	}

	/**
	 * トランザクション分離レベル
	 */
	public static enum IsolationLevel {
		SERIALIZABLE,
		READ_COMMITTED;
	}

	/**
	 * 現在のConfig値のときの契約マスタのブロックサイズを取得する
	 *
	 * @return
	 */
	public int getContractBlockSize() {
		return duplicatePhoneNumberRate * 2 + expirationDateRate + noExpirationDateRate;
	}

	/**
	 *
	 * @param システムプロパティpropertyで指定したファイルを使用してコンフィグを作成し、Config.configForAppCOnfigにセットする。
	 *
	 * @param requiredFile ファイル指定が必須の場合true
	 * @throws IOException
	 *
	 * TODO コンパイルエラーが取れたらメソッド名をgetCOnfigに変更する
	 */
	public static Config setConfigForAppConfig(boolean requiredFile) throws IOException {
        String s = System.getProperty("property");
        if (s == null) {
        	if (requiredFile) {
        		throw new RuntimeException("not found -Dproperty=property-file-path");
        	}
        }
        return Config.getConfig(s);
	}



	/**
	 * @return dbManager
	 */
	public synchronized PhoneBillDbManager getDbManager() {
		if (dbManager == null) {
			switch (dbmsType) {
			default:
				throw new UnsupportedOperationException("unsupported dbms type: " + dbmsType);
			case ORACLE_JDBC:
				dbManager = new PhoneBillDbManagerOracle(this);
				break;
			case POSTGRE_SQL_JDBC:
				dbManager = new PhoneBillDbManagerPostgresql(this);
				break;
			}
			LOG.info("using " + dbManager.getClass().getSimpleName());
		}
		return dbManager;
	}

	public PhoneBillDbManagerJdbc getDbManagerJdbc() {
		return (PhoneBillDbManagerJdbc) getDbManager();
	}
}
