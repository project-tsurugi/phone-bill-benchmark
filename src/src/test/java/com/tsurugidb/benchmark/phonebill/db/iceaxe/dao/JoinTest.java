package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.AbstractJdbcTestCase;
import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeTestTools;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * PostgreSQLとIceaxeでJOINして検索の結果が同じことを確認する
 */
public class JoinTest extends AbstractJdbcTestCase {
	private static final String ICEAXE_CONFIG_PATH = "src/test/config/iceaxe.properties";

	private static PhoneBillDbManagerJdbc managerPostgresql;
	private static Config configPostgresql;
	private static Config configIceaxe;

	private static IceaxeTestTools iceaxeTestTools;

	// テストデータ
	private static Contract C1 = Contract.create("001", "2022-01-01", "2024-09-25", "dummy");
	private static Contract C2 = Contract.create("002", "2022-01-01", null, "dummy");
	private static Contract C3 = Contract.create("899", "2022-01-01", null, "dummy");
	private static Contract C4 = Contract.create("999", "2022-01-01", null, "dummy");

	// 契約c1の履歴データ
	private static History H1 = History.create("001", "002", "C", "2022-03-05 12:10:01.999", 1, null, 0);
	private static History H2 = History.create("001", "005", "C", "2022-03-05 12:10:11.999", 2, null, 0);
	private static History H3 = History.create("001", "009", "C", "2022-03-06 12:10:01.999", 3, null, 0);

	// 契約c2の履歴データ
	private static History H4 = History.create("002", "005", "C", "2022-03-05 12:10:01.999", 6, null, 0);
	private static History H5 = History.create("002", "001", "C", "2022-05-05 12:10:01.999", 5, null, 0);
	private static History H6 = History.create("002", "007", "C", "2022-09-15 12:10:01.999", 4, null, 0);

	// 契約c1, c2意外の履歴データ
	private static History H7 = History.create("003", "005", "C", "2022-06-05 12:10:01.999", 6, null, 0);
	private static History H8 = History.create("004", "001", "C", "2022-01-05 12:10:01.999", 5, null, 0);
	private static History H9 = History.create("005", "007", "C", "2022-09-18 12:10:01.999", 4, null, 0);

	private static Set<History> histories = new HashSet<>();
	private static Set<Contract> contracts = new HashSet<>();

	private static List<History> EXPECTED = new ArrayList<>();


	// テスト対象のSQL

	private String SQL = "select h.caller_phone_number, h.recipient_phone_number, "
			+ " h.payment_category, h.start_time, h.time_secs, h.charge, h.df from history h"
			+ " inner join contracts c on c.phone_number = h.caller_phone_number"
			+ " where c.start_date < h.start_time and h.start_time < c.end_date + 1"
			+ " and c.phone_number = '001' "
			+ " order by h.start_time";

	@BeforeAll
	protected static void setUpBeforeClass2() throws Exception {

		configPostgresql = Config.getConfig();
		configIceaxe = Config.getConfig(ICEAXE_CONFIG_PATH);
		iceaxeTestTools = new IceaxeTestTools(configIceaxe);

		contracts.add(C1);
		contracts.add(C2);
		contracts.add(C3);
		contracts.add(C4);
		histories.add(H1);
		histories.add(H2);
		histories.add(H3);
		histories.add(H4);
		histories.add(H5);
		histories.add(H6);
		histories.add(H7);
		histories.add(H8);
		histories.add(H9);

		EXPECTED.add(H1);
		EXPECTED.add(H2);
		EXPECTED.add(H3);
	}

	@AfterAll
	protected static void tearDownAfterClass2() throws Exception {
		iceaxeTestTools.close();
		if (managerPostgresql != null) {
			managerPostgresql.close();
		}
	}


	private void preparePostgresql() {
		PhoneBillDbManager manager = getManagerPostgresql();
		Ddl ddl = manager.getDdl();
		manager.execute(TxOption.of(), () -> {
			ddl.dropTable("contracts");
			ddl.dropTable("history");
			ddl.createContractsTable();
			ddl.createHistoryTable();
			manager.getContractDao().batchInsert(contracts);
			manager.getHistoryDao().batchInsert(histories);
		});
	}

	private void prepareIceaxe() {
		Ddl ddl = getManagerIceaxe().getDdl();
		// 空のテーブルを作成する
		if (iceaxeTestTools.tableExists("history")) {
			iceaxeTestTools.execute(() -> ddl.dropTable("history"));
		}
		if (iceaxeTestTools.tableExists("contracts")) {
			iceaxeTestTools.execute(() -> ddl.dropTable("contracts"));
		}
		iceaxeTestTools.execute(ddl::createHistoryTable);
		iceaxeTestTools.execute(ddl::createContractsTable);
		iceaxeTestTools.insertToHistory(histories);
		iceaxeTestTools.insertToContracts(contracts);
	}

	@Test
	@SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	public void testPostgreSQL() throws SQLException {
		preparePostgresql();
		assertEquals(histories, new HashSet<History>(getHistories()));
		assertEquals(contracts, new HashSet<Contract>(getContracts()));

		try (ResultSet rs = getStmt().executeQuery(SQL)) {
			List<History> actual = new ArrayList<>();
			while (rs.next()) {
				History h = new History();
				h.setCallerPhoneNumber(rs.getString(1));
				h.setRecipientPhoneNumber(rs.getString(2));
				h.setPaymentCategorty(rs.getString(3));
				h.setStartTime(rs.getTimestamp(4));
				h.setTimeSecs(rs.getInt(5));
				int charge = rs.getInt(6);
				h.setCharge(rs.wasNull() ? null : charge);
				h.setDf(rs.getInt(7));
				actual.add(h);
			}
			assertEquals(EXPECTED, actual);
		}
	}

	@Test
	@Disabled // 2022/09/01 現在正しく動作しない => ps.execute()で固まる。
	public void testIceaxe() {
		prepareIceaxe();
		assertEquals(histories, iceaxeTestTools.getHistorySet());
		assertEquals(contracts, iceaxeTestTools.getContractSet());
		String sql = "select h.caller_phone_number, h.recipient_phone_number, "
				+ " h.payment_category, h.start_time, h.time_secs, h.charge, h.df from history h"
				+ " inner join contracts c on c.phone_number = h.caller_phone_number"
				+ " where c.start_date < h.start_time and h.start_time < c.end_date + 1"
				+ " and c.phone_number = '001' "
				+ " order by h.start_time";

		var resultMapping =
				TgResultMapping.of(History::new)
				.addString("caller_phone_number", History::setCallerPhoneNumber)
				.addString("recipient_phone_number", History::setRecipientPhoneNumber)
				.addString("payment_category", History::setPaymentCategorty)
				.addDateTime("start_time", History::setStartTime)
				.addInt("time_secs", History::setTimeSecs)
				.addInt("charge", History::setCharge)
				.addInt("df", History::setDf);

		PhoneBillDbManagerIceaxe manager = iceaxeTestTools.getManager();

		List<History> actual = iceaxeTestTools.execute(() -> {
			try (var ps = manager.getSession().createQuery(sql, resultMapping)) {
				try (var result = ps.execute(manager.getCurrentTransaction())) {
					return result.getRecordList();
				} catch (TsurugiTransactionException e) {
					throw new TsurugiTransactionRuntimeException(e);
				}
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		});
		assertEquals(EXPECTED, actual);
	}

	private static PhoneBillDbManagerJdbc getManagerPostgresql() {
		if (managerPostgresql == null) {
			managerPostgresql = (PhoneBillDbManagerJdbc) PhoneBillDbManager.createPhoneBillDbManager(configPostgresql);
		}
		return managerPostgresql;
	}

	private static PhoneBillDbManagerIceaxe getManagerIceaxe() {
		return iceaxeTestTools.getManager();
	}

}
