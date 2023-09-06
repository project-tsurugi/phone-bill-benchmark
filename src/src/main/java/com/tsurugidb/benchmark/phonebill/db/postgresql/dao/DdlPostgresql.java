package com.tsurugidb.benchmark.phonebill.db.postgresql.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;
import com.tsurugidb.benchmark.phonebill.db.jdbc.dao.DdlJdbc;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class DdlPostgresql extends DdlJdbc {
    private Connection conn;

    public DdlPostgresql(PhoneBillDbManagerJdbc manager) {
        super(manager);
        conn = manager.getConnection();
    }

    public void createHistoryTable() {
        String create_table = "create table history ("
                + "caller_phone_number varchar(15) not null," 		// 発信者電話番号
                + "recipient_phone_number varchar(15) not null," 	// 受信者電話番号
                + "payment_category char(1) not null," 			// 料金区分
                + "start_time timestamp not null,"			 		// 通話開始時刻
                + "time_secs integer not null," 					// 通話時間(秒)
                + "charge integer," 								// 料金
                + "df integer not null" 							// 論理削除フラグ
                + ") with (fillfactor=80)";
        execute(create_table);
    }

    @Override
    public void prepareLoadData() {
            long startTime = System.currentTimeMillis();
            execute("truncate table history");
            execute("truncate table contracts");
            commit();

            dropPrimaryKey("history", "history_pkey");
            dropPrimaryKey("contracts", "contracts_pkey");
            dropIndex("idx_st");
            dropIndex("idx_rp");
            commit();
            long elapsedTime = System.currentTimeMillis() - startTime;
            String format = "Truncate teable and drop indexies in %,.3f sec ";
            LOG.info(String.format(format, elapsedTime / 1000d));
    }

    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    @Override
    public void createIndexes() {
        createIndexes(true);
    }

    protected void createIndexes(boolean createSecondaryIndexes) {
        long startTime = System.currentTimeMillis();
        if (createSecondaryIndexes) {
            executeWithLogging("create index idx_st on history(start_time)");
            executeWithLogging("create index idx_rp on history(recipient_phone_number, payment_category, start_time)");
        }
        executeWithLogging(
                "alter table history add constraint history_pkey primary key (caller_phone_number, payment_category, start_time)");
        executeWithLogging(
                "alter table contracts add constraint contracts_pkey primary key (phone_number, start_date)");
        long elapsedTime = System.currentTimeMillis() - startTime;
        String format = "Create indexies in %,.3f sec ";
        LOG.info(String.format(format, elapsedTime / 1000d));
    }


    /**
     * 指定のテーブルからPrimaryKeyを削除する
     *
     * @param table
     * @param pk
     */
    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    private void dropPrimaryKey(String table, String pk) {
        execute("alter table " + table + " drop constraint if exists " + pk);
    }

    /**
     * 指定のテーブルから指定のIndexを削除する
     *
     * @param index
     */
    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    protected void dropIndex(String index) {
        execute("drop index if exists " + index);
    }

    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    @Override
    public void updateStatistics() {
        long startTime = System.currentTimeMillis();
        execute("analyze history");
        execute("analyze contracts");
        long elapsedTime = System.currentTimeMillis() - startTime;
        String format = "Update statistic in %,.3f sec ";
        LOG.info(String.format(format, elapsedTime / 1000d));
    }

    @Override
    public void dropTable(String tableName) {
        execute("drop table if exists " + tableName);
    }

    @Override
    public boolean tableExists(String tableName) {
        String sql = "SELECT table_name FROM information_schema.tables WHERE table_name = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
