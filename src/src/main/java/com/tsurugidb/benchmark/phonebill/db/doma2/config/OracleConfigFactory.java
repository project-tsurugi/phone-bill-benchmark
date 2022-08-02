package com.tsurugidb.benchmark.phonebill.db.doma2.config;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.seasar.doma.jdbc.dialect.Dialect;
import org.seasar.doma.jdbc.dialect.OracleDialect;

import com.tsurugidb.benchmark.phonebill.app.Config;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

public class OracleConfigFactory implements JdbcConfigFactory {

    @Override
    public DataSource createDataSource(Config config) {
        PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
        try {
            pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
            pds.setURL(config.url);
            pds.setUser(config.user);
            pds.setPassword(config.password);
            pds.setMaxStatements(256);
            return pds;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Dialect createDialect(Config config) {
        return new OracleDialect();
    }

}
