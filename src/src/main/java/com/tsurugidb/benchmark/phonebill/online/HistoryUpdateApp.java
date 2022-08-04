package com.tsurugidb.benchmark.phonebill.online;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TgTmSettingDummy;
import com.tsurugidb.benchmark.phonebill.db.doma2.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.doma2.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.doma2.entity.History;
import com.tsurugidb.benchmark.phonebill.testdata.CallTimeGenerator;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.ContractInfoReader;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class HistoryUpdateApp extends AbstractOnlineApp {
	private static final Logger LOG = LoggerFactory.getLogger(HistoryUpdateApp.class);
	private ContractInfoReader contractInfoReader;
	private CallTimeGenerator callTimeGenerator;
	private Updater[] updaters = { new Updater1(), new Updater2() };
	private History history;
	private Random random;
	private HistoryDao historyDao;
	private PhoneBillDbManager manager;

	public HistoryUpdateApp(Config config, Random random, ContractBlockInfoAccessor accessor)
			throws SQLException, IOException {
		super(config.historyUpdateRecordsPerMin, config, random);
		this.callTimeGenerator = CallTimeGenerator.createCallTimeGenerator(random, config);
		this.contractInfoReader = ContractInfoReader.create(config, accessor, random);
		this.random = random;
		manager = config.getDbManager();
		historyDao = manager.getHistoryDao();
	}

	void updateDatabase(History history) {
		manager.execute(TgTmSettingDummy.getInstance(), () -> historyDao.update(history));
	}

	/**
	 * 発信者電話番号と通話開始時刻が指定の契約に合致するレコードを取得する
	 *
	 * @param key
	 * @return
	 * @throws SQLException
	 */
	List<History> getHistories(Key key) {
		return manager.execute(TgTmSettingDummy.getInstance(), () -> historyDao.getHistories(key));
	}

	@Override
	protected void createData() {
		List<History> histories = Collections.emptyList();
		while (histories.isEmpty()) {
			// 更新対象となる契約を選択
			Key key = contractInfoReader.getKeyUpdatingContract();

			// 通話履歴テーブルから、当該契約の有効期間内に当該契約の電話番号で発信した履歴を取り出す
			if (skipDatabaseAccess) {
				return;
			}
			histories = getHistories(key);
		}

		// 取り出した履歴から1レコードを取り出し更新する
		history = histories.get(random.nextInt(histories.size()));

		Updater updater = updaters[random.nextInt(updaters.length)];
		updater.update(history);
	}

	@Override
	protected void updateDatabase() {
		updateDatabase(history);
		LOG.debug("ONLINE APP: Update 1 record from history(callerPhoneNumber = {}, startTime = {}).",
				history.callerPhoneNumber, history.startTime);
	}

	/**
	 * スケジュール作成時に、契約マスタのブロック情報をアップデートする
	 *
	 * @throws IOException
	 */
	@Override
	protected void atScheduleListCreated(List<Long> scheduleList) throws IOException {
		// TODO UT追加
		contractInfoReader.loadActiveBlockNumberList();
	}

	// 通話履歴を更新するInterfaceと、Interfaceを実装したクラス

	interface Updater {
		/**
		 * 通話履歴を更新する
		 *
		 * @param history
		 */
		void update(History history);
	}

	/**
	 * 削除フラグを立てる
	 *
	 */
	@SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC")
	class Updater1 implements Updater {

		@Override
		public void update(History history) {
			history.df = 1;
			history.charge = null;
		}
	}

	/**
	 * 通話時間を変更する
	 */
	class Updater2 implements Updater {
		@Override
		public void update(History history) {
			history.timeSecs = callTimeGenerator.getTimeSecs();
			history.charge = null;
		}
	}
}
