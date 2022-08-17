package com.tsurugidb.benchmark.phonebill.online;

import java.io.IOException;
import java.sql.Date;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TgTmSettingDummy;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.ContractInfoReader;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class MasterUpdateApp extends AbstractOnlineApp {
    private static final Logger LOG = LoggerFactory.getLogger(MasterUpdateApp.class);

	private static final long DAY_IN_MILLS = 24 * 3600 * 1000;

	private ContractInfoReader contractInfoReader;
	private Config config;
	private PhoneBillDbManager manager;
	private ContractDao dao;
	private Random random;
	private Updater[] updaters = {new Updater1(), new Updater2()};

	/**
	 * コンストラクタ
	 *
	 * @param accessor
	 * @param config
	 * @param seed
	 * @throws IOException
	 */
	public MasterUpdateApp(Config config, Random random, ContractBlockInfoAccessor accessor) throws IOException {
		super(config.masterUpdateRecordsPerMin, config, random);
		this.config = config;
		this.random = random;
		this.contractInfoReader = ContractInfoReader.create(config, accessor, random);
		manager = PhoneBillDbManager.createPhoneBillDbManager(config);
		dao = manager.getContractDao();
	}


	/**
	 * 同一の電話番号の契約のリストから、任意の契約を一つ選択し契約内容を更新する.
	 * <br>
	 * ランダムに選んだ契約をランダムに更新し更新後の値に矛盾がないか調べる。矛盾がある場合は別の値で更新して、
	 * 矛盾がないかを調べる。100回更新しても、矛盾のない値にならなかった場合はnullを返す。
	 *
	 * @param contracts 同一の電話番号の契約のリスト
	 * @param key
	 * @return 更新した契約
	 */
	private Contract getUpdatingContract(List<Contract> contracts, Key key) {
		for (int i = 0; i < 100; i++) {
			// 契約を一つ選択して更新する
			Set<Contract> set = new HashSet<Contract>(contracts);
			Contract orgContract = contracts.get(random.nextInt(contracts.size()));
			set.remove(orgContract);
			Contract contract = orgContract.clone();
			Updater updater = updaters[random.nextInt(updaters.length)];
			updater.update(contract);
			// 契約期間の重複がなければOK
			if (!commonDuration(contract, set)) {
				return contract;
			}
		}
		return null;
	}


	/**
	 * 指定の契約と、契約のコレクションの契約期間に共通の月がないかを調べる
	 *
	 * @param c
	 * @param contracts
	 * @return 共通の月があるときtrue
	 */
	static boolean commonDuration(Contract contract, Collection<Contract> contracts) {
		long start = toEpochMonth(contract.startDate);
		long end = toEpochMonth(contract.endDate);
		for(Contract target: contracts) {
			long targetStart = toEpochMonth(target.startDate);
			long targetEnd = toEpochMonth(target.endDate);
			if (start <= targetEnd && targetStart <= end) {
				return true;
			}
		}
		return false;
	}

	private static long toEpochMonth(Date date) {
		return date == null ? Long.MAX_VALUE : date.toLocalDate().withDayOfMonth(1).toEpochDay();
	}


	/**
	 * 指定の契約を取得する
	 *
	 * @param key
	 * @return
	 */
	List<Contract> getContracts(String phoneNumber) {
		return manager.execute(TgTmSettingDummy.getInstance(), ()->dao.getContracts(phoneNumber));
	}

	@Override
	protected void createData() {
		// Nothing to do
	}

	@Override
	protected void updateDatabase() {

		// 更新対象の電話番号を取得
		Key key = contractInfoReader.getKeyUpdatingContract();

		if (skipDatabaseAccess) {
			return;
		}

		// 当該電話番号の契約を取得
		List<Contract> contracts = getContracts(key.phoneNumber);
		if (contracts.isEmpty()) {
			// 電話番号にマッチする契約がなかったとき(DBに追加される前の電話番号を選んだ場合) => 基本的にありえない
			LOG.warn("No contract found for phoneNumber = {}.", key.phoneNumber);
			return;
		}

		// 取得した契約から更新対象の契約を更新する
		Contract updatingContract = getUpdatingContract(contracts, key);
		if (updatingContract == null) {
			// 契約の更新に失敗したとき(DBが壊れている可能性が高い)
			LOG.warn("Fail to create valid update contracts for phone number: {}", contracts.get(0).phoneNumber);
			for(Contract c: contracts) {
				LOG.warn("   " + c.toString());
			}
			return;
		}

		// 契約を更新

		int ret = manager.execute(TgTmSettingDummy.getInstance(), () -> dao.update(updatingContract));
		if (ret != 1) {
			// select ～ updateの間に対象レコードが削除されたケース -> 基本的にありえない
			throw new RuntimeException("Fail to update contracts: " + updatingContract);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("ONLINE APP: Update 1 record from contracs(phoneNumber = {}, startDate = {}, endDate = {}).",
					updatingContract.phoneNumber, updatingContract.startDate, updatingContract.endDate);
		}
	}

	/**
	 * スケジュール作成時に、契約マスタのブロック情報をアップデートする
	 * @throws IOException
	 */
	@Override
	protected void atScheduleListCreated(List<Long> scheduleList) throws IOException {
		contractInfoReader.loadActiveBlockNumberList();
	}


	// 契約を更新するInterfaceと、Interfaceを実装したクラス
	interface Updater {
		/**
		 * Contactの値を更新する
		 *
		 * @param contract
		 */
		void update(Contract contract);
	}

	/**
	 * 契約終了日を削除する
	 *
	 */
	@SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC")
	class Updater1 implements Updater {
		@Override
		public void update(Contract contract) {
			contract.endDate = null;
		}
	}

	/**
	 * 契約終了日を設定する
	 *
	 */
	class Updater2 implements Updater {
		@Override
		public void update(Contract contract) {
			long startTime = contract.startDate.getTime();
			int d = (int) ((config.maxDate.getTime() - startTime) / DAY_IN_MILLS);
			int r = random.nextInt(d + 1);
			contract.endDate = new Date(startTime + r * DAY_IN_MILLS);
		}
	}
}
