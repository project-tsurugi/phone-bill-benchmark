package com.tsurugidb.benchmark.phonebill.db;

import com.tsurugidb.iceaxe.transaction.TgTmSetting;

/**
 * Iceaxe対応開始するまで、TgTmSettingのインスタンスが必要になったときに渡すダミークラス.
 * <br>
 * TODO Iceaxe対応時にこのクラスを使用しているコードをすべて書き換える。
 *
 */
public class TgTmSettingDummy extends TgTmSetting {
	public static TgTmSettingDummy getInstance() {
		return new TgTmSettingDummy();
	}
}
