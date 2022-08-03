package com.tsurugidb.benchmark.phonebill.db;

import java.io.Closeable;
import java.sql.Connection;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.interfaces.DdlLExecutor;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;
import com.tsurugidb.benchmark.phonebill.db.oracle.PhoneBillDbManagerOracle;
import com.tsurugidb.benchmark.phonebill.db.postgresql.PhoneBillDbManagerPostgresql;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;

public abstract class PhoneBillDbManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneBillDbManager.class);


	public static PhoneBillDbManager createInstance(Config config) {
		PhoneBillDbManagerJdbc manager;
		switch (config.dbmsType) {
		default:
			throw new UnsupportedOperationException("unsupported dbms type: " + config.dbmsType);
		case ORACLE_JDBC:
			manager = new PhoneBillDbManagerOracle(config);
			break;
		case POSTGRE_SQL_JDBC:
			manager = new PhoneBillDbManagerPostgresql(config);
			break;
		}
		LOG.info("using " + manager.getClass().getSimpleName());
		return manager;
    }

	public abstract DdlLExecutor getDdlLExecutor();

    public abstract void execute(TgTmSetting setting, Runnable runnable);

    public abstract <T> T execute(TgTmSetting setting, Supplier<T> supplier);

    public final void commit() {
        commit(null);
    }

    public abstract void commit(Runnable listener);

    public final void rollback() {
        rollback(null);
    }

    public abstract void rollback(Runnable listener);

    @Override
    public abstract void close();

	public abstract Connection getConnection();
}
