package com.tsurugidb.benchmark.phonebill.db.doma2.config;

import org.seasar.doma.SingletonConfig;
import org.seasar.doma.jdbc.dialect.Dialect;
import org.seasar.doma.jdbc.tx.LocalTransactionDataSource;
import org.seasar.doma.jdbc.tx.LocalTransactionManager;
import org.seasar.doma.jdbc.tx.TransactionManager;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.Config.DbmsType;

// https://doma.readthedocs.io/en/2.5.1/transaction/
@SingletonConfig
public class AppConfig implements org.seasar.doma.jdbc.Config {
    private static final AppConfig CONFIG = new AppConfig();
    private final Dialect dialect;
    private final LocalTransactionDataSource dataSource;
    private final TransactionManager transactionManager;


	private AppConfig() {
		Config config = Config.getConfigForAppConfig();
		JdbcConfigFactory configFactory = createConfigFactory(config.dbmsType);
		this.dialect = configFactory.createDialect(config);
		this.dataSource = new LocalTransactionDataSource(configFactory.createDataSource(config));
		this.transactionManager = new LocalTransactionManager(dataSource.getLocalTransaction(getJdbcLogger()));
	}

	private JdbcConfigFactory createConfigFactory(DbmsType dbmsType) {
		switch (dbmsType) {
		case ORACLE_JDBC:
			return new OracleConfigFactory();
		case POSTGRE_SQL_JDBC:
			return new PostgresConfigFactory();
		default:
			throw new UnsupportedOperationException("unsupported dbms type: " + dbmsType);

		}
    }

    @Override
    public Dialect getDialect() {
        return dialect;
    }

    @Override
    public LocalTransactionDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public static AppConfig singleton() {
        return CONFIG;
    }
}
