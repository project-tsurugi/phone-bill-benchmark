package com.tsurugidb.benchmark.phonebill.db.doma2;

import java.sql.Connection;
import java.util.function.Supplier;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.interfaces.DdlLExecutor;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;

public class PhoneBillDbManagerDoma2 extends PhoneBillDbManager {

	public PhoneBillDbManagerDoma2(Config config) {
		// TODO 自動生成されたコンストラクター・スタブ
	}

	@Override
	public void execute(TgTmSetting setting, Runnable runnable) {
		// TODO 自動生成されたメソッド・スタブ

	}

	@Override
	public <T> T execute(TgTmSetting setting, Supplier<T> supplier) {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}

	@Override
	public void close() {
		// TODO 自動生成されたメソッド・スタブ

	}

	@Override
	public DdlLExecutor getDdlLExecutor() {
		throw new UnsupportedOperationException("PhoneBillDbManager for Doma2 does not support DDL execution.");
	}

	@Override
	public void commit(Runnable listener) {
		// TODO 自動生成されたメソッド・スタブ

	}

	@Override
	public void rollback(Runnable listener) {
		// TODO 自動生成されたメソッド・スタブ

	}

	@Override
	public Connection getConnection() {
		throw new UnsupportedOperationException("PhoneBillDbManager for Doma2 does not support getConnection method.");
	}

}
