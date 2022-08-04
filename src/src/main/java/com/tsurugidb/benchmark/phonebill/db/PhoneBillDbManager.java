package com.tsurugidb.benchmark.phonebill.db;

import java.io.Closeable;
import java.util.function.Supplier;

import com.tsurugidb.benchmark.phonebill.db.doma2.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.doma2.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.interfaces.DdlLExecutor;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;

public abstract class PhoneBillDbManager implements Closeable {
	public abstract DdlLExecutor getDdlLExecutor();
	public abstract ContractDao getContractDao();
	public abstract HistoryDao getHistoryDao();


    public abstract void execute(TgTmSetting setting, Runnable runnable);

    public abstract <T> T execute(TgTmSetting setting, Supplier<T> supplier);

    public final void commit() {
        commit(null);
    }

    public abstract void commit(Runnable listener);

    public final void rollback() {
        rollback(null);
    }

    public abstract void rollback(Runnable listener);

    @Override
    public abstract void close();

}
