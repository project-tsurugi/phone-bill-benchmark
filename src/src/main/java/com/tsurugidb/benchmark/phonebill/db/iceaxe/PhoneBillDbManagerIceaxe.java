package com.tsurugidb.benchmark.phonebill.db.iceaxe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.dao.BillingDao;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.DdlIceaxe;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.HistoryDaoIceaxe;
import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;

public class PhoneBillDbManagerIceaxe extends PhoneBillDbManager {
    private final TsurugiSession session;
    private final TsurugiTransactionManager transactionManager;
    private final ThreadLocal<TsurugiTransaction> transactionThreadLocal = new ThreadLocal<>();

	public PhoneBillDbManagerIceaxe(Config config) {
        var endpoint = config.url;
        var connector = TsurugiConnector.createConnector(endpoint);
        try {
            var info = TgSessionInfo.of(config.user, config.password);
            this.session = connector.createSession(info);
            this.transactionManager = session.createTransactionManager();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
	}

    public TsurugiSession getSession() {
        return this.session;
    }


	private Ddl ddl;

	@Override
	public Ddl getDdl() {
		if (ddl == null) {
			ddl = new DdlIceaxe(this);
		}
		return ddl;
	}

	@Override
	public ContractDao getContractDao() {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}

	private HistoryDao historyDao;

	@Override
	public HistoryDao getHistoryDao() {
		if (historyDao == null) {
			historyDao = new HistoryDaoIceaxe(this);
		}
		return historyDao;
	}

	@Override
	public BillingDao getBillingDao() {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}



    @Override
    public void execute(TgTmSetting setting, Runnable runnable) {
        try {
            transactionManager.execute(setting, transaction -> {
                transactionThreadLocal.set(transaction);
                try {
                    runnable.run();
                } finally {
                    transactionThreadLocal.remove();
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public <T> T execute(TgTmSetting setting, Supplier<T> supplier) {
        try {
            return transactionManager.execute(setting, transaction -> {
                transactionThreadLocal.set(transaction);
                try {
                    return supplier.get();
                } finally {
                    transactionThreadLocal.remove();
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public TsurugiTransaction getCurrentTransaction() {
        return transactionThreadLocal.get();
    }

    @Override
    public void commit(Runnable listener) {
        if (listener != null) {
            var transaction = getCurrentTransaction();
            transaction.addCommitListener(listener);
        }
    }


    @Override
    public void rollback(Runnable listener) {
        var transaction = getCurrentTransaction();
        if (listener != null) {
            transaction.addRollbackListener(listener);
        }
        try {
            transaction.rollback();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            session.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

	@Override
	public boolean isRetriable(Throwable t) {
		// TODO 自動生成されたメソッド・スタブ
		return false;
	}

}
