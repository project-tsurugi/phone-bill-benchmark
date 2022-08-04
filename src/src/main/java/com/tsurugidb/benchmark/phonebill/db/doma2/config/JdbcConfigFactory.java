package com.tsurugidb.benchmark.phonebill.db.doma2.config;

import javax.sql.DataSource;

import com.tsurugidb.benchmark.phonebill.app.Config;

interface JdbcConfigFactory {

    DataSource createDataSource(Config config);


}
