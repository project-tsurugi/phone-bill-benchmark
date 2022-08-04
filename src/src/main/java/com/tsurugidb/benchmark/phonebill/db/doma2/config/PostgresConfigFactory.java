package com.tsurugidb.benchmark.phonebill.db.doma2.config;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;

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


}
