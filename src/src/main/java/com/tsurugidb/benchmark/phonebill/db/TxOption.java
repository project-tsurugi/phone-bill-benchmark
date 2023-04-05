package com.tsurugidb.benchmark.phonebill.db;

import static  com.tsurugidb.benchmark.phonebill.db.TxLabel.*;

import java.util.HashMap;
import java.util.Map;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager.CounterKey;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager.CounterName;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.transaction.option.TgTxOptionLtx;

/**
 * PhoneBillDbManager使用時に指定するTxOption.
 *
 */
/**
 *
 */
public class TxOption {
	/**
	 * リトライ可能な例外が返った場合にリトライする回数の上限
	 */
	private final int retryCountLmit;

	/**
	 * Iceaxeのトランザクションセッティング、Iceaxe以外では無効。
	 */
	private final TgTmSetting setting;


	/**
	 * Iceaxeのトランザクションオプション、Iceaxe以外では無効。
	 */
	private final TgTxOption tgTxOption;

	/**
	 * トランザクションに付けるラベル、Iceaxe以外では無効
	 */
	private final TxLabel label;


	/**
	 * CounterKeyのキャッシュ
	 */
	private Map<CounterName, CounterKey> counterkeyCache = new HashMap<>();


	private TxOption(int retryCountLmit, TxLabel label, TgTxOption option) {
		option.label(label.toString());
		this.retryCountLmit = retryCountLmit;
		this.label = label;
		this.tgTxOption = option;
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
		return ofOCC(retryCountLmit, DEFAULT);
    }



    /**
     * create transaction option for OCC
     *
     * @param retryCountLmit retry count limit
     * @param label label for transaction
     * @return
     */
    public static TxOption ofOCC(int retryCountLmit,  TxLabel label) {
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
    public static TxOption ofLTX(int retryCountLmit,  TxLabel label, Table... writePreserveTables) {
    	TgTxOptionLtx tgTxOption = TgTxOption.ofLTX();
    	for (Table table: writePreserveTables) {
    		tgTxOption.addWritePreserve(table.getTableName());
    	}
        return new TxOption(retryCountLmit, label, tgTxOption);
    }


    /**
     * create transaction option for read only transaction
     *
     * @param retryCountLmit retry count limit
     * @param label label for transaction
     * @return transaction option
     */
    public static TxOption ofRTX(int retryCountLmit,  TxLabel label) {
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
	 * 指定の名称のgetCounterKeyを取得する
	 *
	 * @param name
	 * @return
	 */
	public CounterKey getCounterKey(CounterName name) {
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

	@Override
	public String toString() {
		return tgTxOption.toString();
	}
}
