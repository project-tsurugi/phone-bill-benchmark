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


	private TxOption(int retryCountLmit, String label, TgTxOption option) {
		option.label(label);
		this.retryCountLmit = retryCountLmit;
		this.label = label;
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
        return new  TxOption(retryCountLmit, label, TgTxOption.ofOCC());
    }

    /**
     * create transaction option for long transaction
     *
     * @param retryCountLmit retry count limit
     * @param label label for transaction
     * @param writePreserveTableNames table name to Write Preserve
     * @return transaction option
     */
    public static TxOption ofLTX(int retryCountLmit,  String label, Table... writePreserveTables) {
    	TgTxOption tgTxOption = TgTxOption.ofLTX();
    	for (Table table: writePreserveTables) {
    		tgTxOption.addWritePreserve(table.getTableName());
    	}
        return new  TxOption(retryCountLmit, label, tgTxOption);
    }


    /**
     * create transaction option for read only transaction
     *
     * @param retryCountLmit retry count limit
     * @param label label for transaction
     * @return transaction option
     */
    public static TxOption ofRTX(int retryCountLmit,  String label) {
        return new  TxOption(retryCountLmit, label, TgTxOption.ofRTX());
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

	/**
	 * ofLTX()で指定するテーブルを定義する列挙型
	 */
	public static enum Table {
		HISTORY("history"),
		CONTRACTS("contracts"),
		BILLING("billing");

		private String tableName;

		private Table(String tableName) {
			this.tableName = tableName;
		}

		/**
		 * @return tableName
		 */
		public String getTableName() {
			return tableName;
		}
	}
}
