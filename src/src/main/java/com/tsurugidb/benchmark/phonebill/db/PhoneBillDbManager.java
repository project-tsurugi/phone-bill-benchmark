package com.tsurugidb.benchmark.phonebill.db;

import java.io.Closeable;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.dao.BillingDao;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.oracle.PhoneBillDbManagerOracle;
import com.tsurugidb.benchmark.phonebill.db.postgresql.PhoneBillDbManagerPostgresql;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;

public abstract class PhoneBillDbManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneBillDbManager.class);

	public abstract Ddl getDdl();
	public abstract ContractDao getContractDao();
	public abstract HistoryDao getHistoryDao();
	public abstract BillingDao getBillingDao();


    public abstract void execute(TgTmSetting setting, Runnable runnable);

    public abstract <T> T execute(TgTmSetting setting, Supplier<T> supplier);

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
    public abstract void commit(Runnable listener);



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
    public abstract void rollback(Runnable listener);


    /**
     * 管理しているすべてのコネクションをクローズする
     */
    @Override
    public abstract void close();

	/**
	 * 指定のThrowableを調べ、リトライにより回復可能な場合はtrueを返す
	 *
	 * @param e
	 * @return
	 */
	public abstract boolean isRetriable(Throwable t);


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
		}
		LOG.info("using " + dbManager.getClass().getSimpleName());
		return dbManager;
	}

	public static PhoneBillDbManager createPhoneBillDbManager(Config config) {
		return createPhoneBillDbManager(config, SessionHoldingType.THREAD_LOCAL);
	}

}
