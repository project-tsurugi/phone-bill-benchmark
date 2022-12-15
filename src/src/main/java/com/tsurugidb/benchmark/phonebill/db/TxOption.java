package com.tsurugidb.benchmark.phonebill.db;

import java.util.HashMap;
import java.util.Map;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager.CounterKey;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

/**
 * PhoneBillDbManager使用時に指定するTxOption.
 *
 */
public class TxOption {
	/**
	 * リトライ可能な例外が返った場合にリトライする回数の上限
	 */
	private int retryCountLmit;

	/**
	 * Iceaxeのトランザクションセッティング、Iceaxe以外では無効。
	 */
	private TgTmSetting setting;


	/**
	 * トランザクションに付けるラベル、Iceaxe以外では無効
	 */
	private String label;


	/**
	 * CounterKeyのキャッシュ
	 */
	private Map<String, CounterKey> counterkeyCache = new HashMap<>();


	private TxOption(int retryCountLmit, TgTxOption option, String label) {
		option.label(label);
		this.retryCountLmit = retryCountLmit;
		this.setting = retryCountLmit == 0 ? TgTmSetting.of(option) : TgTmSetting.ofAlways(option, retryCountLmit);
	}


    /**
     * create transaction option
     *
     * @return transaction option
     */
    public static TxOption of() {
		return of(0);
    }

    /**
     * create transaction option
     *
     * @param retryCountLmit retry count limit
     * @return transaction option
     */
    public static TxOption of(int retryCountLmit) {
		return ofOCC(retryCountLmit, "default");
    }



    /**
     * create transaction option for OCC
     *
     * @param retryCountLmit retry count limit
     * @param label label for transaction
     * @return
     */
    public static TxOption ofOCC(int retryCountLmit,  String label) {
        return new  TxOption(retryCountLmit, TgTxOption.ofOCC(), label);
    }

    /**
     * create transaction option for long transaction
     *
     * @param retryCountLmit retry count limit
     * @param label label for transaction
     * @param writePreserveTableNames table name to Write Preserve
     * @return transaction option
     */
    public static TxOption ofLTX(int retryCountLmit,  String label, String... writePreserveTableNames) {
        return new  TxOption(retryCountLmit, TgTxOption.ofLTX(writePreserveTableNames), label);
    }


    /**
     * create transaction option for read only transaction
     *
     * @param retryCountLmit retry count limit
     * @param label label for transaction
     * @return transaction option
     */
    public static TxOption ofRTX(int retryCountLmit,  String label) {
        return new  TxOption(retryCountLmit, TgTxOption.ofRTX(), label);
    }


	/**
	 * @return retryCountLmit
	 */
	public int getRetryCountLimit() {
		return retryCountLmit;
	}

	/**
	 * @return settingIceaxe
	 */
	public TgTmSetting getSettingIceaxe() {
		return setting;
	}


	/**
	 * @return label
	 */
	public String getLabel() {
		return label;
	}


	/**
	 * 指定の名称のgetCounterKeyを取得する
	 *
	 * @param name
	 * @return
	 */
	public CounterKey getCounterKey(String name) {
		CounterKey key = counterkeyCache.get(name);
		if (key == null) {
			key = new CounterKey(label, name);
			counterkeyCache.put(name, key);
		}
		return key;
	}
}
