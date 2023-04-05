package com.tsurugidb.benchmark.phonebill.db;

import static java.nio.file.StandardOpenOption.*;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.dao.BillingDao;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxeSurrogateKey;
import com.tsurugidb.benchmark.phonebill.db.oracle.PhoneBillDbManagerOracle;
import com.tsurugidb.benchmark.phonebill.db.postgresql.PhoneBillDbManagerPostgresql;
import com.tsurugidb.benchmark.phonebill.db.postgresql.PhoneBillDbManagerPostgresqlNoBatchUpdate;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 *
 */
public abstract class PhoneBillDbManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneBillDbManager.class);

    // カウンタを格納するmap
    protected static final Map<CounterKey, AtomicInteger> ccounterMap = new ConcurrentHashMap<>();

	// リトライするExceptionを集計するためのコレクション

	public static final Collection<Exception> retryingExceptions = new ConcurrentLinkedQueue<>();


	// クローズ漏れチェック用のマップ

	private static Map<PhoneBillDbManager, String> stackTraceMap = new ConcurrentHashMap<>();



    // DAO取得用のメソッド
	public abstract Ddl getDdl();
	public abstract ContractDao getContractDao();
	public abstract HistoryDao getHistoryDao();
	public abstract BillingDao getBillingDao();

    /**
     * トランザクションを実行する
     *
     * @param setting
     * @param runnable
     */
    public abstract void execute(TxOption setting, Runnable runnable);


    /**
     * トランザクションを実行する
     *
     * @param <T>
     * @param setting
     * @param supplier
     * @return
     */
    public abstract <T> T execute(TxOption setting, Supplier<T> supplier);

    /**
     * トランザクションをコミットする
     *
     */
    public final void commit() {
        commit(null);
    }

    /**
     * トランザクションをコミットし、ロールバックに成功したときに
     * listenerを呼び出す。
     *
     * @param listener
     */
    public abstract void commit(Consumer<TsurugiTransaction> listener);



    /**
     * トランザクションをロールバックする。
     */
    public final void rollback() {
        rollback(null);
    }

    /**
     * トランザクションをロールバックし、ロールバックに成功したときに
     * listenerを呼び出す。
     *
     * @param listener
     */
    public abstract void rollback(Consumer<TsurugiTransaction> listener);


    /**
     * 管理しているすべてのコネクションをクローズする
     */
    @Override
    public final void close() {
    	doClose();
    	stackTraceMap.remove(this);
    }


    /**
     * 派生クラスのクローズ処理
     */
    protected abstract void doClose();


	/**
	 * セッションの保持方法を示すenum
	 */
	public enum SessionHoldingType {
		THREAD_LOCAL,
		INSTANCE_FIELD
	}

	public static PhoneBillDbManager createPhoneBillDbManager(Config config, SessionHoldingType type) {
		PhoneBillDbManager dbManager;
		switch (config.dbmsType) {
		default:
			throw new UnsupportedOperationException("unsupported dbms type: " + config.dbmsType);
		case ORACLE_JDBC:
			dbManager = new PhoneBillDbManagerOracle(config, type);
			break;
		case POSTGRE_SQL_JDBC:
			dbManager = new PhoneBillDbManagerPostgresql(config, type);
			break;
		case POSTGRE_NO_BATCHUPDATE:
			dbManager = new PhoneBillDbManagerPostgresqlNoBatchUpdate(config, type);
			break;
		case ICEAXE:
			dbManager = new PhoneBillDbManagerIceaxe(config);
			break;
		case ICEAXE_SURROGATE_KEY:
			dbManager = new PhoneBillDbManagerIceaxeSurrogateKey(config);
			break;
		}
		LOG.debug("using " + dbManager.getClass().getSimpleName());
		// クローズ漏れのレポート用にスタックトレースを記録する
		StringWriter sw = new StringWriter();
		try (PrintWriter pw = new PrintWriter(sw)) {
			new Exception("Stack trace").printStackTrace(pw);
		}
		stackTraceMap.put(dbManager, sw.toString());
		return dbManager;
	}

	public static PhoneBillDbManager createPhoneBillDbManager(Config config) {
		return createPhoneBillDbManager(config, SessionHoldingType.THREAD_LOCAL);
	}


	/**
	 * 実行中、または最後に実行したトランザクションのトランザクションIDを取得する。
	 * トランザクションIDを取得できない場合は文字列"none"を返す。
	 *
	 * @return トランザクションID
	 */
	public String getTransactionId() {
		return "none";
	}


	/**
	 * 指定のTxOption, カウンタ名のカウンタをカウントアップする
	 *
	 * @param option
	 * @param name
	 */
	public final void countup(TxOption option, CounterName name ) {
		CounterKey key = option.getCounterKey(name);
		AtomicInteger counter = ccounterMap.get(key);
		if (counter == null) {
			counter = new AtomicInteger(0);
			ccounterMap.put(key, counter);
		}
		counter.incrementAndGet();
	}




	/**
	 * カウンタのカウント値を取得する。カウンタが存在しない場合は0を返す。
	 *
	 * @param key カウンタのキー
	 * @return カウント値
	 */
	public static int getCounter(CounterKey key) {
		AtomicInteger counter = ccounterMap.get(key);
		if (counter == null) {
			return 0;
		}
		return counter.intValue();
	}


	/**
	 * カウンタを初期化する。
	 *
	 */
	public static void initCounter() {
		ccounterMap.clear();
	}


	/**
	 * counterMapで使用するキー
	 */
	public static class CounterKey {
		/**
		 * トランザクションのラベル
		 */
		TxLabel label;
		/**
		 * カウンタの名称
		 */
		CounterName name;


		/**
		 * コンストラクタ
		 *
		 * @param label
		 * @param name
		 */
		CounterKey(TxLabel label, CounterName name) {
			this.label = label;
			this.name = name;
		}

		public static CounterKey of(TxLabel label, CounterName name) {
			return new CounterKey(label, name);
		}


		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((label == null) ? 0 : label.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			CounterKey other = (CounterKey) obj;
			if (label != other.label)
				return false;
			if (name != other.name)
				return false;
			return true;
		}
	}

	public static enum CounterName {
		// For DB Manager
		BEGIN_TX,
		TRY_COMMIT,
		SUCCESS,
		ABORTED,

		// For Online App
		OCC_TRY,
		OCC_ABORT,
		OCC_SUCC,
		OCC_ABANDONED_RETRY,
		LTX_TRY,
		LTX_ABORT,
		LTX_SUCC,
		LTX_ABANDONED_RETRY
	}



	/**
	 * カウンタのレポートを作成する
	 *
	 * @return レポートの文字列
	 */
	public static String createCounterReport() {
		// 使用されているラベルととカウンタ名をリストアップ
		final Set<CounterName> counterNames = new LinkedHashSet<>();
		counterNames.add(CounterName.BEGIN_TX);
		counterNames.add(CounterName.TRY_COMMIT);
		counterNames.add(CounterName.ABORTED);
		counterNames.add(CounterName.SUCCESS);
		final List<TxLabel> labels = ccounterMap.keySet().stream().map(k -> k.label).distinct().sorted()
				.collect(Collectors.toList());

		// レポートの作成
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try (PrintStream ps = new PrintStream(bos, true, StandardCharsets.UTF_8)) {
			// ヘッダを生成
			ps.println("TX_LABELS," + String.join(",", counterNames.stream().map(cn -> cn.name()).collect(Collectors.toList())));
			// 本体
			for(TxLabel label: labels) {
				ps.print(label);
				for (CounterName countrName: counterNames) {
					CounterKey key = new CounterKey(label, countrName);
					AtomicInteger c = ccounterMap.get(key);
					ps.print(',');
					ps.print(c == null ? 0: c.get());
				}
				ps.println();
			}
		}
		return bos.toString(StandardCharsets.UTF_8);
	}


	/**
	 * 例外のレポートを作成する
	 *
	 * @return レポートの文字列
	 */
	public static String createExceptionReport() {
		Path outputPath = Path.of("/tmp/outpubt.bin");
		try (ObjectOutputStream os = new ObjectOutputStream(
				Files.newOutputStream(outputPath, CREATE, TRUNCATE_EXISTING, WRITE))) {
			for (Object obj : retryingExceptions) {
				os.writeObject(obj);
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return "Wrote to " + outputPath.toString();
	}

	/**
	 * リトライするExceptionを記録する
	 *
	 * @param e
	 */
	public static void addRetringExceptions(Exception e) {
		retryingExceptions.add(e);
	}


	/**
	 * クローズしていないPhoneBillDbManagerのコレクションを返す
	 *
	 * @return
	 */
	public static Set<PhoneBillDbManager> getNotClosed() {
		return stackTraceMap.keySet();
	}

	/**
	 * クローズしていないPhoneBillDbManagerのレポートを出力する。
	 *
	 */
	public static void reportNotClosed() {
		if (stackTraceMap.isEmpty()) {
			LOG.debug("No leak has been discovered.");
			return;
		}
		for(String stackTrace: stackTraceMap.values()) {
			LOG.error("A leak has been discovered, stack trace = {}.", stackTrace);
		}
	}
}
