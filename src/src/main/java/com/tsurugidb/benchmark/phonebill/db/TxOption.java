package com.tsurugidb.benchmark.phonebill.db;

import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

public class TxOption {
	/**
	 * JDBCでリトライ可能な例外が返った場合にリトライする回数の上限
	 */
	private int retryCountLmitJdbc;
	/**
	 * Iceaxeを使用する場合のトランザクションセッティング
	 */
	private TgTmSetting settingIceaxe;

	private TxOption(int retryCountJdbc, TgTmSetting settingIceaxe) {
		this.retryCountLmitJdbc = retryCountJdbc;
		this.settingIceaxe = settingIceaxe;
	}

	public static TxOption of(TgTmSetting settingIceaxe) {
		return new TxOption(Integer.MAX_VALUE, settingIceaxe);
	}

	public static TxOption of(int retryCountJdbc,  TgTmSetting settingIceaxe) {
		return new TxOption(retryCountJdbc, settingIceaxe);
	}

	public static TxOption of() {
		return new TxOption(Integer.MAX_VALUE, TgTmSetting.of(TgTxOption.ofOCC()));
	}


	/**
	 * @return retryCountLmitJdbc
	 */
	public int getRetryCountJdbc() {
		return retryCountLmitJdbc;
	}

	/**
	 * @return settingIceaxe
	 */
	public TgTmSetting getSettingIceaxe() {
		return settingIceaxe;
	}


}
