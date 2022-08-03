package com.tsurugidb.benchmark.phonebill.db.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;

public abstract class PhoneBillDbManagerJdbc extends PhoneBillDbManager{
    private static final Logger LOG = LoggerFactory.getLogger(PhoneBillDbManagerJdbc.class);
    private Config config;

    private final List<Connection> connectionList = new CopyOnWriteArrayList<>();

    private final ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<Connection>() {
        @Override
        protected Connection initialValue() {
            try {
                Connection c = createConnection();
                connectionList.add(c);
                return c;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    };



    public PhoneBillDbManagerJdbc(Config config) {
    	this.config = config;
	}


    protected abstract Connection createConnection() throws SQLException;

    public Connection getConnection() {
        return connectionThreadLocal.get();
    }

    @Override
    public void commit(Runnable listener) {
        Connection c = getConnection();
        try {
            c.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (listener != null) {
            listener.run();
        }
    }

    @Override
    public void rollback(Runnable listener) {
        Connection c = getConnection();
        try {
            c.rollback();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (listener != null) {
            listener.run();
        }
    }

    @Override
    public void close() {
        RuntimeException exception = null;

        for (Connection c : connectionList) {
            try {
                c.close();
            } catch (SQLException e) {
                if (exception == null) {
                    exception = new RuntimeException(e);
                } else {
                    exception.addSuppressed(e);
                }
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public void execute(TgTmSetting setting, Runnable runnable) {
        try {
            runnable.run();
            commit();
        } catch (Throwable e) {
            try {
                rollback();
            } catch (Throwable t) {
                e.addSuppressed(t);
            }
            throw e;
        }
    }

    @Override
    public <T> T execute(TgTmSetting setting, Supplier<T> supplier) {
        try {
            T r = supplier.get();
            commit();
            return r;
        } catch (Throwable e) {
            try {
                rollback();
            } catch (Throwable t) {
                e.addSuppressed(t);
            }
            throw e;
        }
    }
}
