package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;

public class DdlIceaxeSurrogateKey extends DdlIceaxe {

	public DdlIceaxeSurrogateKey(PhoneBillDbManagerIceaxe phoneBillDbManagerIceaxe) {
		super(phoneBillDbManagerIceaxe);
	}

	@Override
	public void createHistoryTable() {
		String create_table = "create table history ("
				+ "sid bigint, "
				+ "caller_phone_number varchar(15) not null," // 発信者電話番号
				+ "recipient_phone_number varchar(15) not null," // 受信者電話番号
				+ "payment_category char(1) not null," // 料金区分
				+ "start_time timestamp not null," // 通話開始時刻
				+ "time_secs int not null," // 通話時間(秒)
				+ "charge int," // 料金
				+ "df int not null," // 論理削除フラグ
				+ "primary key (sid)" + ")";
		execute(create_table);
		execute("create index idx_hst on history(df)");
		execute("create index idx_st on history(start_time)");
		execute("create index idx_rp on history(recipient_phone_number, payment_category, start_time)");
		execute("create index idx_npk on history(caller_phone_number, payment_category, start_time)");
	}
}
