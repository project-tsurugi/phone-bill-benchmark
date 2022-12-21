package com.tsurugidb.benchmark.phonebill.db.iceaxe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.RetryOverRuntimeException;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.BillingDao;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.BillingDaoIceaxe;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.ContractDaoIceaxe;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.DdlIceaxe;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.HistoryDaoIceaxe;
import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRetryOverIOException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;

public class PhoneBillDbManagerIceaxe extends PhoneBillDbManager {
    private final TsurugiSession session;
    private final TsurugiTransactionManager transactionManager;
    private final ThreadLocal<TsurugiTransaction> transactionThreadLocal = new ThreadLocal<>();
    private final Config config;

	public PhoneBillDbManagerIceaxe(Config config) {
		this.config = config;
		var endpoint = config.url;
        var connector = TsurugiConnector.createConnector(endpoint);
        try {
            var info = TgSessionInfo.of();
            this.session = connector.createSession(info);
            session.setConnectTimeout(30, TimeUnit.SECONDS);
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

	private ContractDao contractDao;

	@Override
	public ContractDao getContractDao() {
		if (contractDao == null) {
			contractDao = new ContractDaoIceaxe(this);
		}
		return contractDao;
	}

	private HistoryDao historyDao;

	@Override
	public HistoryDao getHistoryDao() {
		if (historyDao == null) {
			historyDao = new HistoryDaoIceaxe(this);
		}
		return historyDao;
	}

	private BillingDao billingDao;

	@Override
	public BillingDao getBillingDao() {
		if (billingDao == null) {
			billingDao = new BillingDaoIceaxe(this);
		}
		return billingDao;
	}



    @Override
    public void execute(TxOption txOption, Runnable runnable) {
    	TgTmSetting setting = txOption.getSettingIceaxe();
        try {
            transactionManager.execute(setting, transaction -> {
                transactionThreadLocal.set(transaction);
                try {
                	countup(txOption, BEGIN_TX);
                    runnable.run();
                	countup(txOption, TRY_COMMIT);
                } finally {
                    transactionThreadLocal.remove();
                }
            });
        } catch (IOException e) {
            countup(txOption, ABORTED);
        	if (isRetriable(e)) {
            	throw new RetryOverRuntimeException(e);
        	}
            throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
            countup(txOption, ABORTED);
        	if (isRetriable(e)) {
            	throw new RetryOverRuntimeException(e);
        	}
        	throw e;
        }
        countup(txOption, SUCCESS);
    }

    @Override
    public <T> T execute(TxOption txOption, Supplier<T> supplier) {
    	TgTmSetting setting = txOption.getSettingIceaxe();
        try {
            return transactionManager.execute(setting, transaction -> {
                transactionThreadLocal.set(transaction);
                try {
                	countup(txOption, BEGIN_TX);
                    var ret =  supplier.get();
                	countup(txOption, TRY_COMMIT);
                	return ret;

                } finally {
                    transactionThreadLocal.remove();
                }
            });
        } catch (IOException e) {
            countup(txOption, ABORTED);
        	if (isRetriable(e)) {
            	throw new RetryOverRuntimeException(e);
        	}
            throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
            countup(txOption, ABORTED);
        	if (isRetriable(e)) {
            	throw new RetryOverRuntimeException(e);
        	}
        	throw e;
        }
    }

    public TsurugiTransaction getCurrentTransaction() {
        return transactionThreadLocal.get();
    }

    @Override
    public void commit(Consumer<TsurugiTransaction> listener) {
        if (listener != null) {
            var transaction = getCurrentTransaction();
            transaction.addCommitListener(listener);
        }
    }


    @Override
    public void rollback(Consumer<TsurugiTransaction> listener) {
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

	/**
	 * @return config
	 */
	public Config getConfig() {
		return config;
	}

	@Override
	public String getTransactionId() {
		try {
			String txId = getCurrentTransaction().getTransactionId();
			return txId;
		} catch (NoSuchElementException | IOException e) {
			return "none";
		}
	}

	/**
	 * Throwableがリトライ可能か調べる
	 */
	private boolean isRetriable(Throwable t) {
		// TODO: これはとりあえず動くコードでしかない。OLTPが返すエラーコードが整理されたら、それを反映したコードにする
		while (t != null) {
			if (t instanceof TsurugiTransactionRetryOverIOException) {
				return true;
			}
			if (t instanceof SqlServiceException) {
				return true;
			}
			t = t.getCause();
		}
		return false;
	}

}
