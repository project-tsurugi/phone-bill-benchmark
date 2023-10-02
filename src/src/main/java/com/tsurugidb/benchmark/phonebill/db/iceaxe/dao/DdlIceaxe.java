package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
import com.tsurugidb.iceaxe.metadata.TgTableMetadata;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;

/**
 * TODO Tsurguiがまだcreate index, alter table...をサポートしていないため。createIndexes, updateStatistics,
 * afterLoadData, prepareLoadDataは未実装。また、createIndexesではなくcreate tableでPKを作成している．
 *
 */
public class DdlIceaxe implements Ddl {
    private final PhoneBillDbManagerIceaxe manager;

    public DdlIceaxe(PhoneBillDbManagerIceaxe phoneBillDbManagerIceaxe) {
        this.manager = phoneBillDbManagerIceaxe;
    }

    @Override
    public void dropTable(String tableName) {
        Optional<TgTableMetadata> opt;
        try {
            opt = manager.getSession().findTableMetadata(tableName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (opt.isPresent()) {
            execute("drop table " + tableName);
        }
    }

    @Override
    public void createHistoryTable() {
        String create_table = "create table history (" + "caller_phone_number varchar(15) not null," // 発信者電話番号
                + "recipient_phone_number varchar(15) not null," // 受信者電話番号
                + "payment_category char(1) not null," // 料金区分
                + "start_time timestamp not null," // 通話開始時刻
                + "time_secs int not null," // 通話時間(秒)
                + "charge int," // 料金
                + "df int not null," // 論理削除フラグ
                + "primary key (caller_phone_number, payment_category, start_time)" + ")";
        execute(create_table);
        execute("create index idx_st on history(start_time)");
        execute("create index idx_rp on history(recipient_phone_number, payment_category, start_time)");
    }

    @Override
    public void createContractsTable() {
        String create_table = "create table contracts ("
                + "phone_number varchar(15) not null," 		// 電話番号
                + "start_date date not null," 				// 契約開始日
                + "end_date date,"							// 契約終了日
                + "charge_rule varchar(255) not null,"		// 料金計算ルール
                + "primary key (phone_number, start_date)"
                + ")";
        execute(create_table);
    }

    @Override
    public void createBillingTable() {
        String create_table = "create table billing ("
                + "phone_number varchar(15) not null," 					// 電話番号
                + "target_month date not null," 						// 対象年月
                + "basic_charge int not null," 						// 基本料金
                + "metered_charge int not null,"					// 従量料金
                + "billing_amount int not null,"					// 請求金額
                + "batch_exec_id varchar(36) not null,"					// バッチ実行ID
                + "primary key(target_month, phone_number, batch_exec_id)"
                + ")";
        execute(create_table);
    }

    //

    @Override
    public void createIndexes() {
//      TODO 現在create indexは、テーブルが空の時のみ実行可能なので、create tableと同時に実行する
//		execute("create index history(df)");
//		execute("create index idx_st on history(start_time)");
//		execute("create index idx_rp on history(recipient_phone_number, start_time)");
//		execute(
//				"alter table history add constraint history_pkey " + "primary key (caller_phone_number, start_time)");
//		execute(
//				"alter table contracts add constraint contracts_pkey " + "primary key (phone_number, start_date)");

    }

    @Override
    public void updateStatistics() {
    }

    @Override
    public void afterLoadData() {
    }

    @Override
    public void prepareLoadData() {
        dropTables();
        createContractsTable();
        createHistoryTable();
        createBillingTable();
    }

    protected void execute(String sql) {
        try {
            manager.getCurrentTransaction().executeDdl(sql);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    @Override
    public boolean tableExists(String tableName) {
        try {
            var opt = manager.getSession().findTableMetadata(tableName);
            return opt.isPresent();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
