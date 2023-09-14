package com.tsurugidb.benchmark.phonebill.db.oracle.dao;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;
import com.tsurugidb.benchmark.phonebill.db.jdbc.dao.DdlJdbc;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class DdlOracle extends DdlJdbc {
    private Config config;
    private PhoneBillDbManagerJdbc manager;

    public DdlOracle(PhoneBillDbManagerJdbc manager, Config config) {
        super(manager);
        this.config = config;
        this.manager = manager;
    }

    @Override
    public void createHistoryTable() {
        String create_table = "create table history ("
                + "caller_phone_number varchar(15) not null," 		// 発信者電話番号
                + "recipient_phone_number varchar(15) not null," 	// 受信者電話番号
                + "payment_category char(1) not null," 			// 料金区分
                + "start_time timestamp not null,"			 		// 通話開始時刻
                + "time_secs integer not null," 					// 通話時間(秒)
                + "charge integer," 								// 料金
                + "df integer not null" // 論理削除フラグ
                + ")";
        if (config.oracleInitran != 0) {
            create_table = create_table + "initrans " + config.oracleInitran;
        }
        execute(create_table);
    }

    @Override
    public void prepareLoadData() {
        long startTime = System.currentTimeMillis();
        execute("truncate table history");
        execute("truncate table contracts");

        dropPrimaryKey("history", "history_pkey");
        dropPrimaryKey("contracts", "contracts_pkey");
        dropIndex("idx_st");
        dropIndex("idx_rp");
        long elapsedTime = System.currentTimeMillis() - startTime;
        String format = "Truncate teable and drop indexies in %,.3f sec ";
        LOG.info(String.format(format, elapsedTime / 1000d));
    }

    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    public void createIndexes() {
        String option = config.oracleCreateIndexOption;
        long startTime = System.currentTimeMillis();
        executeWithLogging("create index idx_st on history(start_time) " + option);
        executeWithLogging("create index idx_rp on history(recipient_phone_number, payment_category, start_time) " + option);
        executeWithLogging("alter table history add constraint history_pkey "
                + "primary key (caller_phone_number, payment_category, start_time) " + option);
        executeWithLogging("alter table contracts add constraint contracts_pkey "
                + "primary key (phone_number, start_date) " + option);
        long elapsedTime = System.currentTimeMillis() - startTime;
        String format = "Create indexies in %,.3f sec ";
        LOG.info(String.format(format, elapsedTime / 1000d));
    }

    /**
     * 指定のテーブルからPrimaryKeyを削除する
     *
     * @param table
     * @param pk
     * @param stmt
     * @param config
     * @throws SQLException
     */
    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    private void dropPrimaryKey(String table, String pk) {
        // ORA-02441: 存在しない主キーを削除することはできませんは無視する
        execute("alter table " + table + " drop primary key", 2441);
    }

    /**
     * 指定のテーブルから指定のIndexを削除する
     *
     * @param index
     * @param stmt
     * @param config
     * @throws SQLException
     */
    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    private void dropIndex(String index) {
        // ORA-01418: 指定した索引は存在しませんは無視する
        execute("drop index " + index, 1418);
    }

    @Override
    public void updateStatistics() {
        long startTime = System.currentTimeMillis();
        execute("{call DBMS_STATS.GATHER_SCHEMA_STATS(ownname => '" + config.user
                + "', cascade => TRUE, no_invalidate => TRUE)}");
        long elapsedTime = System.currentTimeMillis() - startTime;
        String format = "Update statistic in %,.3f sec ";
        LOG.info(String.format(format, elapsedTime / 1000d));
    }

    @Override
    public void dropTable(String tableName) {
        execute("drop table " + tableName, 942);
    }

    @Override
    public boolean tableExists(String tableName) {
        String sql = "SELECT table_name FROM dba_tables WHERE table_name = ?";
        try (PreparedStatement ps = manager.getConnection().prepareStatement(sql)) {
            ps.setString(1, tableName.toUpperCase(Locale.JAPANESE));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
