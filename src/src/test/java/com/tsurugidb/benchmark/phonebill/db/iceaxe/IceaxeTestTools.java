package com.tsurugidb.benchmark.phonebill.db.iceaxe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Supplier;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TgVariableList;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
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
	private static final TgTmSetting TGT_SETTING = TgTmSetting.of(TgTxOption.ofOCC(), TgTxOption.ofRTX());

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
		// TODO テーブルの存在を確認する方法が提供されてから適切な方法に変更する
		// テーブルに大量のデータが存在すると非常に遅くなる。そのようなケースでの使用は想定していない。
		String sql = "select count(*) from " + tableName;

		return execute(() ->{
			try (var ps = session.createPreparedQuery(sql) ) {
				 List<TsurugiResultEntity> list = ps.executeAndGetList(manager.getCurrentTransaction());
				 if (list.size() == 1) {
					 return true;
				 }
			} catch (IOException e) {
				return false;
			} catch (TsurugiTransactionException e) {
				return false;
			}
			assert false;
			return false;
		} );
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
			String start_time, Integer time_secs, int _df) {
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
						startTime.bind(DateUtils.toTimestamp(start_time).getTime()),
						timeSecs.bind(time_secs),
						charge.bind(null),
						df.bind(_df));
				ps.executeAndGetCount(manager.getCurrentTransaction(), param);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (TsurugiTransactionException e) {
				throw new TsurugiTransactionRuntimeException(e);
			}
		});
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
    	manager.execute(TGT_SETTING, runnable);
    }

    public <T> T execute(Supplier<T> supplier) {
    	return manager.execute(TGT_SETTING, supplier);
    }
}
