package com.tsurugidb.benchmark.phonebill.db.iceaxe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TgVariableList;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;

/**
 * IceaxeのUT用のツール.
 * <p>
 * setUpBeforeClass()でインスタンスを作成し、tearDownAfterClass()でクローズして使用してください。
 *
 */
public class IceaxeTestTools {
	private  final PhoneBillDbManagerIceaxe manager;
	private  final TsurugiSession session;

	public IceaxeTestTools(Config conifg) {
		this.manager = (PhoneBillDbManagerIceaxe) PhoneBillDbManager.createPhoneBillDbManager(conifg);
		this.session = manager.getSession();
	}

	public void close() {
		manager.close();
	}

	/**
	 * @return manager
	 */
	public PhoneBillDbManagerIceaxe getManager() {
		return manager;
	}

	/**
	 * @return session
	 */
	public TsurugiSession getSession() {
		return session;
	}


	/**
	 * 指定の名称のテーブルの有無を調べる
	 *
	 * @param tableName
	 * @return テーブルが存在するときtrue
	 */
	public boolean tableExists(String tableName) {
		try {
			var opt = session.findTableMetadata(tableName);
			return opt.isPresent();
		} catch (IOException e1) {
			throw new RuntimeException();
		}
	}

	/**
	 * 指定のテーブルのレコード数をカウントする。テーブル名にwhere句を付けて
	 * 特定の条件にマッチするレコード数をカウントすることもできる。
	 *
	 * @param table
	 * @return
	 */
	public long countRecords(String tableName) {
		String sql = "select count(*) as cnt from " + tableName;
		return (long) execute(() ->{
			try (var ps = session.createPreparedQuery(sql)) {
				TsurugiTransaction transaction = manager.getCurrentTransaction();
				List<TsurugiResultEntity> list = ps.executeAndGetList(transaction);
				if (list.size() == 1) {
					return (Long) list.get(0).getInt8("cnt");
				}
				assert false;
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (TsurugiTransactionException e) {
				throw new TsurugiTransactionRuntimeException(e);
			}
			assert false;
			return 0;
		} );
	}


	/**
	 * 履歴テーブルにレコードを追加する
	 *
	 * @param caller_phone_number 発信者電話番号
	 * @param recipient_phone_number 受信者電話番号
	 * @param payment_categorty	料金区分
	 * @param start_time 通話開始時刻
	 * @param time_secs 通話時間
	 * @param df 論理削除フラグ
	 */
	public void insertToHistory(String caller_phone_number, String recipient_phone_number, String payment_categorty,
			Timestamp start_time, int time_secs, Integer _charge, int _df) {
		String sql = "insert into history(caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs, charge, df) "
				+ "values(:caller_phone_number, :recipient_phone_number, :payment_categorty, :start_time, :time_secs, :charge, :df)";


		execute(() -> {
			var callerPhoneNumber = TgVariable.ofCharacter("caller_phone_number");
			var recipientPhoneNumber = TgVariable.ofCharacter("recipient_phone_number");
			var paymentCategorty = TgVariable.ofCharacter("payment_categorty");
			var startTime = TgVariable.ofInt8("start_time");
			var timeSecs = TgVariable.ofInt4("time_secs");
			var charge = TgVariable.ofInt4("charge");
			var df = TgVariable.ofInt4("df");

			TgVariableList variableList = TgVariableList.of(
					callerPhoneNumber,
					recipientPhoneNumber,
					paymentCategorty,
					startTime,
					timeSecs,
					charge,
					df);
			try (var ps = session.createPreparedStatement(sql, TgParameterMapping.of(variableList))) {
				TgParameterList param = TgParameterList.of(
						callerPhoneNumber.bind(caller_phone_number),
						recipientPhoneNumber.bind(recipient_phone_number),
						paymentCategorty.bind(payment_categorty),
						startTime.bind(start_time.getTime()),
						timeSecs.bind(time_secs),
						charge.bind(_charge),
						df.bind(_df));
				ps.executeAndGetCount(manager.getCurrentTransaction(), param);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (TsurugiTransactionException e) {
				throw new TsurugiTransactionRuntimeException(e);
			}
		});
	}

	public void insertToHistory(String caller_phone_number, String recipient_phone_number, String payment_categorty,
			String start_time, int time_secs, Integer _charge, int _df) {
		insertToHistory(caller_phone_number, recipient_phone_number, payment_categorty,
				DateUtils.toTimestamp(start_time), time_secs, _charge, _df);
	}

	/**
	 * 履歴テーブルにレコードを追加する
	 *
	 * @param histories
	 */
	public void insertToHistory(History... histories) {
		insertToHistory(Arrays.asList(histories));
	}

	/**
	 * 履歴テーブルにレコードを追加する

	 * @param histories
	 */
	public void insertToHistory(Collection<History> histories) {
		for(History h: histories) {
			System.out.println(h);
			insertToHistory(h.getCallerPhoneNumber(), h.getRecipientPhoneNumber(), h.getPaymentCategorty(),
					h.getStartTime(), h.getTimeSecs(), h.getCharge(), h.getDf());
		}
	}


	/**
	 * 契約テーブルにレコードを追加する
	 *
	 * @param contracts
	 */
	public void insertToContracts(Collection<Contract> contracts) {
		String sql = "insert into contracts(" + "phone_number," + "start_date," + "end_date," + "charge_rule"
				+ ") values(:phone_number, :start_date, :end_date, :charge_rule)";
		TgParameterMapping<Contract> param = TgParameterMapping.of(Contract.class)
				.add("phone_number", TgDataType.CHARACTER, Contract::getPhoneNumber)
				.add("start_date", TgDataType.INT8, Contract::getStartDateAsLong)
				.add("end_date", TgDataType.INT8, Contract::getEndDateAsLong)
				.add("charge_rule", TgDataType.CHARACTER, Contract::getRule);
		execute(() -> {
			try (var ps = session.createPreparedStatement(sql, param)) {
				for(Contract c: contracts) {
					ps.executeAndGetCount(manager.getCurrentTransaction(), c);
				}
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (TsurugiTransactionException e) {
				throw new TsurugiTransactionRuntimeException(e);
			}
		});
	}

	/**
	 * 請求テーブルにレコードを追加する
	 *
	 * @param billings
	 */
	public void insertToBilling(Collection<Billing> billings) {
		String sql = "insert into billing("
				+ "phone_number, "
				+ "target_month, "
				+ "basic_charge, "
				+ "metered_charge, "
				+ "billing_amount, "
				+ "batch_exec_id"
				+ ") values("
				+ ":phone_number, "
				+ ":target_month, "
				+ ":basic_charge, "
				+ ":metered_charge, "
				+ ":billing_amount, "
				+ ":batch_exec_id)";
		TgParameterMapping<Billing> param = TgParameterMapping.of(Billing.class)
				.add("phone_number", TgDataType.CHARACTER, Billing::getPhoneNumber)
				.add("target_month", TgDataType.INT8, Billing::getTargetMonthAsLong)
				.add("basic_charge", TgDataType.INT4, Billing::getBasicCharge)
				.add("metered_charge", TgDataType.INT4, Billing::getMeteredCharge)
				.add("billing_amount", TgDataType.INT4, Billing::getBillingAmount)
				.add("batch_exec_id", TgDataType.CHARACTER, Billing::getBatchExecId);

		execute(() -> {
			try (var ps = session.createPreparedStatement(sql, param)) {
				for(Billing b: billings) {
					ps.executeAndGetCount(manager.getCurrentTransaction(), b);
				}
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (TsurugiTransactionException e) {
				throw new TsurugiTransactionRuntimeException(e);
			}
		});
	}

	/**
	 * 請求テーブルにレコードを追加する
	 *
	 * @param billings
	 */
	public void insertToBilling(Billing... billings) {
		insertToBilling(Arrays.asList(billings));
	}


	/**
	 * 契約テーブルにレコードを追加する
	 *
	 * @param contracts
	 */
	public void insertToContracts(Contract... contracts) {
		insertToContracts(Arrays.asList(contracts));
	}


	/**
	 * 履歴テーブルの全レコードのリストを取得する。
	 *
	 * @return
	 */
	public List<History> getHistryList() {
		var resultMapping =
				TgResultMapping.of(History::new)
				.character("caller_phone_number", History::setCallerPhoneNumber)
				.character("recipient_phone_number", History::setRecipientPhoneNumber)
				.character("payment_categorty", History::setPaymentCategorty)
				.int8("start_time", History::setStartTime)
				.int4("time_secs", History::setTimeSecs)
				.int4("charge", History::setCharge)
				.int4("df", History::setDf);
		return execute(() -> {
			String sql = "select caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs, charge, df from history ";
			try (var ps = session.createPreparedQuery(sql, resultMapping)) {
				try (var result = ps.execute(manager.getCurrentTransaction())) {
					return result.getRecordList();
				}
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (TsurugiTransactionException e) {
				throw new TsurugiTransactionRuntimeException(e);
			}
		});
	}

	/**
	 * 履歴テーブルの全レコードのセットのセットを取得する。

	 * @return
	 */
	public Set<History> getHistorySet() {
		return new HashSet<>(getHistryList());
	}

	/**
	 * 請求テーブルの全レコードのリストを取得する
	 *
	 * @return
	 */
	public List<Billing> getBillingList() {
		var resultMapping =
				TgResultMapping.of(Billing::new)
				.character("phone_number", Billing::setPhoneNumber)
				.int8("target_month", Billing::setTargetMonth)
				.int4("basic_charge", Billing::setBasicCharge)
				.int4("metered_charge", Billing::setMeteredCharge)
				.int4("billing_amount", Billing::setBillingAmount)
				.character("batch_exec_id", Billing::setBatchExecId);
		return execute(() -> {
			String sql = "select phone_number, target_month, basic_charge, metered_charge, billing_amount, batch_exec_id from billing";
			try (var ps = session.createPreparedQuery(sql, resultMapping)) {
				try (var result = ps.execute(manager.getCurrentTransaction())) {
					return result.getRecordList();
				}
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (TsurugiTransactionException e) {
				throw new TsurugiTransactionRuntimeException(e);
			}
		});
	}


	/**
	 * 請求テーブルの全レコードのセットを取得する
	 *
	 * @return
	 */
	public Set<Billing> getBillingSet() {
		return new HashSet<Billing>(getBillingList());
	}


	/**
	 * 契約マスタの全レコードのリストを取得する。
	 *
	 * @return
	 */
	public List<Contract> getContractList() {
		String sql = "select phone_number, start_date, end_date, charge_rule"
				+ " from contracts order by phone_number, start_date";

		var resultMapping =
				TgResultMapping.of(Contract::new)
				.character("phone_number", Contract::setPhoneNumber)
				.int8("start_date", Contract::setStartDate)
				.int8("end_date", Contract::setEndDate)
				.character("charge_rule", Contract::setRule);
		return execute(() -> {
			try (var ps = session.createPreparedQuery(sql, resultMapping)) {
				try (var result = ps.execute(manager.getCurrentTransaction())) {
					return result.getRecordList();
				}
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (TsurugiTransactionException e) {
				throw new TsurugiTransactionRuntimeException(e);
			}
		});
	}

	/**
	 * 契約マスタの全レコードのセットを取得する。
	 *
	 * @return
	 */
	public Set<Contract> getContractSet() {
		return new HashSet<Contract>(getContractList());
	}


	/**
	 * 指定のsqlを実行する。
	 *
	 * @param sql
	 */
	public void execute(String sql) {
		execute(() -> {
			try (var ps = session.createPreparedStatement(sql)) {
				TsurugiTransaction transaction = manager.getCurrentTransaction();
				ps.executeAndGetCount(transaction);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (TsurugiTransactionException e) {
				throw new TsurugiTransactionRuntimeException(e);
			}
		});

	}


    public void execute(Runnable runnable) {
    	manager.execute(PhoneBillDbManager.OCC_RTX, runnable);
    }

    public <T> T execute(Supplier<T> supplier) {
    	return manager.execute(PhoneBillDbManager.OCC_RTX, supplier);
    }
}
