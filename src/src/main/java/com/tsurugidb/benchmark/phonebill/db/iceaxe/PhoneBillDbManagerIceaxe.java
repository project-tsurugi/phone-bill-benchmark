/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.event.TsurugiTransactionEventListener;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmRetryOverIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;

public class PhoneBillDbManagerIceaxe extends PhoneBillDbManager {
    private final TsurugiSession session;
    private final TsurugiTransactionManager transactionManager;
    private final ThreadLocal<TsurugiTransaction> transactionThreadLocal = new ThreadLocal<>();
    private final Config config;
    private final InsertType insertType;

    public PhoneBillDbManagerIceaxe(Config config, InsertType insertType) {
        this.config = config;
        this.insertType = insertType;
        var endpoint = config.url;
        var connector = TsurugiConnector.of(endpoint);
        try {
            var sessionOption = TgSessionOption.of();
            this.session = connector.createSession(sessionOption);
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
                    countup(txOption, CounterName.BEGIN_TX);
                    runnable.run();
                    countup(txOption, CounterName.TRY_COMMIT);
                } finally {
                    transactionThreadLocal.remove();
                }
            });
        } catch (IOException e) {
            countup(txOption, CounterName.ABORTED);
            if (isRetriable(e)) {
                throw new RetryOverRuntimeException(e);
            }
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            countup(txOption, CounterName.ABORTED);
            throw new RuntimeException(e);
        } catch (RuntimeException e) {
            countup(txOption, CounterName.ABORTED);
            if (isRetriable(e)) {
                throw new RetryOverRuntimeException(e);
            }
            throw e;
        }
        countup(txOption, CounterName.SUCCESS);
    }

    @Override
    public <T> T execute(TxOption txOption, Supplier<T> supplier) {
        TgTmSetting setting = txOption.getSettingIceaxe();
        try {
            return transactionManager.execute(setting, transaction -> {
                transactionThreadLocal.set(transaction);
                try {
                    countup(txOption, CounterName.BEGIN_TX);
                    var ret =  supplier.get();
                    countup(txOption, CounterName.TRY_COMMIT);
                    return ret;

                } finally {
                    transactionThreadLocal.remove();
                }
            });
        } catch (IOException e) {
            countup(txOption, CounterName.ABORTED);
            if (isRetriable(e)) {
                throw new RetryOverRuntimeException(e);
            }
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            countup(txOption, CounterName.ABORTED);
            throw new RuntimeException(e);
        } catch (RuntimeException e) {
            countup(txOption, CounterName.ABORTED);
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
            transaction.addEventListener(new TsurugiTransactionEventListener() {
                @Override
                public void commitEnd(TsurugiTransaction transaction, TgCommitType commitType, Throwable occurred) {
                    if (occurred == null) {
                        listener.accept(transaction);
                    }
                }
            });
        }
    }

    @Override
    public void rollback(Consumer<TsurugiTransaction> listener) {
        var transaction = getCurrentTransaction();
        if (listener != null) {
            transaction.addEventListener(new TsurugiTransactionEventListener() {
                @Override
                public void rollbackEnd(TsurugiTransaction transaction, Throwable occurred) {
                    if (occurred == null) {
                        listener.accept(transaction);
                    }
                }
            });

        }
        try {
            transaction.rollback();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    @Override
    public void doClose() {
        try {
            session.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
        } catch (NoSuchElementException | IOException | InterruptedException e) {
            return "none";
        }
    }

    /**
     * Throwableがリトライ可能か調べる
     */
    private boolean isRetriable(Throwable t) {
        while (t != null) {
            if (t instanceof TsurugiTmRetryOverIOException) {
                return true;
            }
            if (t instanceof SqlServiceException) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    public InsertType getInsertType() {
        return insertType;
    }

    public static enum InsertType {
        INSERT,
        UPSERT;

        public String getSqlInsertMethod() {
            switch (this) {
                case INSERT:
                    return "insert";
                case UPSERT:
                    return "insert or replace";
                default:
                    throw new IllegalArgumentException("Unknown InsertType: " + this);
            }
        }
    }}
