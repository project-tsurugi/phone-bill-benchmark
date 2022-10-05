package com.tsurugidb.benchmark.phonebill.db;

import java.util.function.Supplier;

import com.tsurugidb.benchmark.phonebill.db.dao.BillingDao;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

/**
 * トランザクションの管理を行わないPhoneBillDbManager。
 * トランザクションスコープの切り替えのために使用する。
 */
public class NoTransactionManagePhoneBillDbManager extends PhoneBillDbManager {
	private PhoneBillDbManager delegatedManager;


	public NoTransactionManagePhoneBillDbManager(PhoneBillDbManager delegatedManager) {
		this.delegatedManager = delegatedManager;
	}

	/**
	 * @return
	 * @see com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager#getDdl()
	 */
	public Ddl getDdl() {
		return delegatedManager.getDdl();
	}

	/**
	 * @return
	 * @see com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager#getContractDao()
	 */
	public ContractDao getContractDao() {
		return delegatedManager.getContractDao();
	}

	/**
	 * @return
	 * @see com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager#getHistoryDao()
	 */
	public HistoryDao getHistoryDao() {
		return delegatedManager.getHistoryDao();
	}

	/**
	 * @return
	 * @see com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager#getBillingDao()
	 */
	public BillingDao getBillingDao() {
		return delegatedManager.getBillingDao();
	}

	/**
	 * @param setting
	 * @param runnable
	 * @see com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager#execute(com.tsurugidb.iceaxe.transaction.manager.TgTmSetting, java.lang.Runnable)
	 */
	public void execute(TgTmSetting setting, Runnable runnable) {
		runnable.run();
	}

	/**
	 * @param <T>
	 * @param setting
	 * @param supplier
	 * @return
	 * @see com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager#execute(com.tsurugidb.iceaxe.transaction.manager.TgTmSetting, java.util.function.Supplier)
	 */
	public <T> T execute(TgTmSetting setting, Supplier<T> supplier) {
		return supplier.get();
	}

	/**
	 * 呼ばれてもなにもしない
	 *
	 * @param listener
	 * @see com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager#commit(java.lang.Runnable)
	 */
	public void commit(Runnable listener) {
		// Nothing to do.
	}

	/**
	 * 呼ばれてもなにもしない
	 *
	 * @param listener
	 * @see com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager#rollback(java.lang.Runnable)
	 */
	public void rollback(Runnable listener) {
		// Nothing to do.
	}

	/**
	 * 呼ばれてもなにもしない
	 *
	 * @see com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager#close()
	 */
	public void close() {
		// Nothing to do.
	}

}
