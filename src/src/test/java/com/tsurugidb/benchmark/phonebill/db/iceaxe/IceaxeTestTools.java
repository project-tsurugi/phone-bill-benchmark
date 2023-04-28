package com.tsurugidb.benchmark.phonebill.db.iceaxe;

import java.io.Closeable;
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
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;

/**
 * IceaxeのUT用のツール.
 * <p>
 * setUpBeforeClass()でインスタンスを作成し、tearDownAfterClass()でクローズして使用してください。
 *
 */
public class IceaxeTestTools implements Closeable {
	private static final TxOption OCC = TxOption.ofOCC(Integer.MAX_VALUE, TxLabel.TEST);
	private  final PhoneBillDbManagerIceaxe manager;
	private  final TsurugiSession session;

	public IceaxeTestTools(Config conifg) {
		this.manager = (PhoneBillDbManagerIceaxe) PhoneBillDbManager.createPhoneBillDbManager(conifg);
		this.session = manager.getSession();
	}

	@Override
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
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException(e);
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
			try (var ps = session.createQuery(sql)) {
				TsurugiTransaction transaction = manager.getCurrentTransaction();
				List<TsurugiResultEntity> list = transaction.executeAndGetList(ps);
				if (list.size() == 1) {
					return list.get(0).getLong("cnt");
				}
				assert false;
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
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
	 * @param payment_category	料金区分
	 * @param start_time 通話開始時刻
	 * @param time_secs 通話時間
	 * @param df 論理削除フラグ
	 */
	public void insertToHistory(String caller_phone_number, String recipient_phone_number, String payment_category,
			Timestamp start_time, int time_secs, Integer _charge, int _df) {
		String sql = "insert into history(caller_phone_number, recipient_phone_number, payment_category, start_time, time_secs, charge, df) "
				+ "values(:caller_phone_number, :recipient_phone_number, :payment_category, :start_time, :time_secs, :charge, :df)";


		execute(() -> {
			var callerPhoneNumber = TgBindVariable.ofString("caller_phone_number");
			var recipientPhoneNumber = TgBindVariable.ofString("recipient_phone_number");
			var paymentCategorty = TgBindVariable.ofString("payment_category");
			var startTime = TgBindVariable.ofDateTime("start_time");
			var timeSecs = TgBindVariable.ofInt("time_secs");
			var charge = TgBindVariable.ofInt("charge");
			var df = TgBindVariable.ofInt("df");

			var parameterMapping = TgParameterMapping.of(
					callerPhoneNumber,
					recipientPhoneNumber,
					paymentCategorty,
					startTime,
					timeSecs,
					charge,
					df);
			try (var ps = session.createStatement(sql, parameterMapping)) {
				TgBindParameters parameter = TgBindParameters.of(
						callerPhoneNumber.bind(caller_phone_number),
						recipientPhoneNumber.bind(recipient_phone_number),
						paymentCategorty.bind(payment_category),
						startTime.bind(start_time.toLocalDateTime()),
						timeSecs.bind(time_secs),
						charge.bind(_charge),
						df.bind(_df));
				manager.getCurrentTransaction().executeAndGetCount(ps, parameter);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			} catch (TsurugiTransactionException e) {
				throw new TsurugiTransactionRuntimeException(e);
			}
		});
	}

	/**
	 * サロゲートキー付きの履歴テーブルにレコードを追加する
	 *
	 * @param histories
	 */
	public void insertToHistoryWsk(History... histories) {
		insertToHistoryWsk(Arrays.asList(histories));
	}

	/**
	 * サロゲートキー付きの契約テーブルにレコードを追加する(start_timeを文字列で指定)
	 *
	 * @param caller_phone_number
	 * @param recipient_phone_number
	 * @param payment_category
	 * @param start_time
	 * @param time_secs
	 * @param _charge
	 * @param _df
	 */
	public void insertToHistoryWsk(long _sid, String caller_phone_number, String recipient_phone_number, String payment_category,
			String start_time, int time_secs, Integer _charge, int _df) {
		insertToHistoryWsk(_sid, caller_phone_number, recipient_phone_number, payment_category,
				DateUtils.toTimestamp(start_time), time_secs, _charge, _df);
	}

	/**
	 * サロゲートキー付きの履歴テーブルにレコードを追加する
	 *
	 * @param caller_phone_number 発信者電話番号
	 * @param recipient_phone_number 受信者電話番号
	 * @param payment_category	料金区分
	 * @param start_time 通話開始時刻
	 * @param time_secs 通話時間
	 * @param df 論理削除フラグ
	 */
	public void insertToHistoryWsk(long _sid, String caller_phone_number, String recipient_phone_number, String payment_category,
			Timestamp start_time, int time_secs, Integer _charge, int _df) {
		String sql = "insert into history(sid, caller_phone_number, recipient_phone_number, payment_category, start_time, time_secs, charge, df) "
				+ "values(:sid, :caller_phone_number, :recipient_phone_number, :payment_category, :start_time, :time_secs, :charge, :df)";


		execute(() -> {
			var sid = TgBindVariable.ofLong("sid");
			var callerPhoneNumber = TgBindVariable.ofString("caller_phone_number");
			var recipientPhoneNumber = TgBindVariable.ofString("recipient_phone_number");
			var paymentCategorty = TgBindVariable.ofString("payment_category");
			var startTime = TgBindVariable.ofDateTime("start_time");
			var timeSecs = TgBindVariable.ofInt("time_secs");
			var charge = TgBindVariable.ofInt("charge");
			var df = TgBindVariable.ofInt("df");

			var parameterMapping = TgParameterMapping.of(
					sid,
					callerPhoneNumber,
					recipientPhoneNumber,
					paymentCategorty,
					startTime,
					timeSecs,
					charge,
					df);
			try (var ps = session.createStatement(sql, parameterMapping)) {
				TgBindParameters parameter = TgBindParameters.of(
						sid.bind(_sid),
						callerPhoneNumber.bind(caller_phone_number),
						recipientPhoneNumber.bind(recipient_phone_number),
						paymentCategorty.bind(payment_category),
						startTime.bind(start_time.toLocalDateTime()),
						timeSecs.bind(time_secs),
						charge.bind(_charge),
						df.bind(_df));
				manager.getCurrentTransaction().executeAndGetCount(ps, parameter);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			} catch (TsurugiTransactionException e) {
				throw new TsurugiTransactionRuntimeException(e);
			}
		});
	}


	/**
	 * 契約テーブルにレコードを追加する(start_timeを文字列で指定)
	 *
	 * @param caller_phone_number
	 * @param recipient_phone_number
	 * @param payment_category
	 * @param start_time
	 * @param time_secs
	 * @param _charge
	 * @param _df
	 */
	public void insertToHistory(String caller_phone_number, String recipient_phone_number, String payment_category,
			String start_time, int time_secs, Integer _charge, int _df) {
		insertToHistory(caller_phone_number, recipient_phone_number, payment_category,
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
			insertToHistory(h.getCallerPhoneNumber(), h.getRecipientPhoneNumber(), h.getPaymentCategorty(),
					h.getStartTime(), h.getTimeSecs(), h.getCharge(), h.getDf());
		}
	}

	/**
	 * 履歴テーブルにレコードを追加する

	 * @param histories
	 */
	public void insertToHistoryWsk(Collection<History> histories) {
		for(History h: histories) {
			insertToHistoryWsk(h.getSid(), h.getCallerPhoneNumber(), h.getRecipientPhoneNumber(), h.getPaymentCategorty(),
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
		var parameterMapping = TgParameterMapping.of(Contract.class)
				.add("phone_number", TgDataType.STRING, Contract::getPhoneNumber)
				.add("start_date", TgDataType.DATE, Contract::getStartDateAsLocalDate)
				.add("end_date", TgDataType.DATE, Contract::getEndDateAsLocalDate)
				.add("charge_rule", TgDataType.STRING, Contract::getRule);
		execute(() -> {
			try (var ps = session.createStatement(sql, parameterMapping)) {
				for(Contract c: contracts) {
					manager.getCurrentTransaction().executeAndGetCount(ps, c);
				}
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
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
		var parameterMapping = TgParameterMapping.of(Billing.class)
				.add("phone_number", TgDataType.STRING, Billing::getPhoneNumber)
				.add("target_month", TgDataType.DATE, Billing::getTargetMonthAsLocalDate)
				.add("basic_charge", TgDataType.INT, Billing::getBasicCharge)
				.add("metered_charge", TgDataType.INT, Billing::getMeteredCharge)
				.add("billing_amount", TgDataType.INT, Billing::getBillingAmount)
				.add("batch_exec_id", TgDataType.STRING, Billing::getBatchExecId);

		execute(() -> {
			try (var ps = session.createStatement(sql, parameterMapping)) {
				for(Billing b: billings) {
					manager.getCurrentTransaction().executeAndGetCount(ps, b);
				}
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
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
	 * サロゲートキー付きの履歴テーブルの全レコードのリストを取得する。
	 *
	 * @return
	 */
	public List<History> getHistoryListWsk() {
		var resultMapping =
				TgResultMapping.of(History::new)
				.addLong("sid", History::setSid)
				.addString("caller_phone_number", History::setCallerPhoneNumber)
				.addString("recipient_phone_number", History::setRecipientPhoneNumber)
				.addString("payment_category", History::setPaymentCategorty)
				.addDateTime("start_time", History::setStartTime)
				.addInt("time_secs", History::setTimeSecs)
				.addInt("charge", History::setCharge)
				.addInt("df", History::setDf);
		return execute(() -> {
			String sql = "select sid, caller_phone_number, recipient_phone_number, payment_category, start_time, time_secs, charge, df from history ";
			try (var ps = session.createQuery(sql, resultMapping)) {
			    return manager.getCurrentTransaction().executeAndGetList(ps);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
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
		return new HashSet<>(getHistoryList());
	}

	/**
	 * 履歴テーブルの全レコードのリストを取得する。
	 *
	 * @return
	 */
	public List<History> getHistoryList() {
		var resultMapping =
				TgResultMapping.of(History::new)
				.addString("caller_phone_number", History::setCallerPhoneNumber)
				.addString("recipient_phone_number", History::setRecipientPhoneNumber)
				.addString("payment_category", History::setPaymentCategorty)
				.addDateTime("start_time", History::setStartTime)
				.addInt("time_secs", History::setTimeSecs)
				.addInt("charge", History::setCharge)
				.addInt("df", History::setDf);
		return execute(() -> {
			String sql = "select caller_phone_number, recipient_phone_number, payment_category, start_time, time_secs, charge, df from history ";
			try (var ps = session.createQuery(sql, resultMapping)) {
			    return manager.getCurrentTransaction().executeAndGetList(ps);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			} catch (TsurugiTransactionException e) {
				throw new TsurugiTransactionRuntimeException(e);
			}
		});
	}

	/**
	 * サロゲートキー付きの履歴テーブルの全レコードのセットのセットを取得する。
	 *
	 * @return
	 */
	public Set<History> getHistorySetWsk() {
		return new HashSet<>(getHistoryListWsk());
	}



	/**
	 * 請求テーブルの全レコードのリストを取得する
	 *
	 * @return
	 */
	public List<Billing> getBillingList() {
		var resultMapping =
				TgResultMapping.of(Billing::new)
				.addString("phone_number", Billing::setPhoneNumber)
				.addDate("target_month", Billing::setTargetMonth)
				.addInt("basic_charge", Billing::setBasicCharge)
				.addInt("metered_charge", Billing::setMeteredCharge)
				.addInt("billing_amount", Billing::setBillingAmount)
				.addString("batch_exec_id", Billing::setBatchExecId);
		return execute(() -> {
			String sql = "select phone_number, target_month, basic_charge, metered_charge, billing_amount, batch_exec_id from billing";
			try (var ps = session.createQuery(sql, resultMapping)) {
				try (var result = ps.execute(manager.getCurrentTransaction())) {
					return result.getRecordList();
				}
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
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
				.addString("phone_number", Contract::setPhoneNumber)
				.addDate("start_date", Contract::setStartDate)
				.addDate("end_date", Contract::setEndDate)
				.addString("charge_rule", Contract::setRule);
		return execute(() -> {
			try (var ps = session.createQuery(sql, resultMapping)) {
				return manager.getCurrentTransaction().executeAndGetList(ps);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
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
			try (var ps = session.createStatement(sql)) {
				TsurugiTransaction transaction = manager.getCurrentTransaction();
				transaction.executeAndGetCount(ps);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			} catch (TsurugiTransactionException e) {
				throw new TsurugiTransactionRuntimeException(e);
			}
		});
	}


	/**
	 * 指定のテーブルをトランケートする。
	 */
	public void truncateTable(String tableName) {
		execute("delete from " + tableName);
	}

    public void execute(Runnable runnable) {
    	manager.execute(OCC, runnable);
    }

    public <T> T execute(Supplier<T> supplier) {
    	return manager.execute(OCC, supplier);
    }
}
