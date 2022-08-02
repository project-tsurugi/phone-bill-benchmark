package com.tsurugidb.benchmark.phonebill.db.doma2.config;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;
import org.seasar.doma.jdbc.dialect.Dialect;
import org.seasar.doma.jdbc.dialect.PostgresDialect;

import com.tsurugidb.benchmark.phonebill.app.Config;

public class PostgresConfigFactory implements JdbcConfigFactory {

    @Override
    public DataSource createDataSource(Config config) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setURL(config.url);
        ds.setUser(config.user);
        ds.setPassword(config.password);
        return ds;
    }

    @Override
    public Dialect createDialect(Config config) {
        return new PostgresDialect();
    }

}
