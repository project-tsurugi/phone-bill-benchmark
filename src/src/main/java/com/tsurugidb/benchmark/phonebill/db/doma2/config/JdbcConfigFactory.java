package com.tsurugidb.benchmark.phonebill.db.doma2.config;

import javax.sql.DataSource;

import org.seasar.doma.jdbc.dialect.Dialect;

import com.tsurugidb.benchmark.phonebill.app.Config;

interface JdbcConfigFactory {

    DataSource createDataSource(Config config);

    Dialect createDialect(Config config);

}
