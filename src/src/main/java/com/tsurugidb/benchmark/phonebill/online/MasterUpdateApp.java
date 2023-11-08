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
import com.tsurugidb.benchmark.phonebill.app.Config.DbmsType;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption.Table;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class MasterUpdateApp extends AbstractOnlineApp {
    private static final Logger LOG = LoggerFactory.getLogger(MasterUpdateApp.class);

    private static final long DAY_IN_MILLS = 24 * 3600 * 1000;

    private Config config;
    private Random random;
    private Updater[] updaters = {new Updater1(), new Updater2()};
    private RandomKeySelector<Contract.Key> keySelector;


    /**
     * コンストラクタ
     *
     * @param accessor
     * @param config
     * @param seed
     * @throws IOException
     */
    public MasterUpdateApp(Config config, Random random, RandomKeySelector<Contract.Key> keySelector) throws IOException {
        super(config.masterUpdateRecordsPerMin, config, random);
        this.config = config;
        this.random = random;
        this.keySelector = keySelector;
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
        long start = toEpochMonth(contract.getStartDate());
        long end = toEpochMonth(contract.getEndDate());
        for(Contract target: contracts) {
            long targetStart = toEpochMonth(target.getStartDate());
            long targetEnd = toEpochMonth(target.getEndDate());
            if (start <= targetEnd && targetStart <= end) {
                return true;
            }
        }
        return false;
    }

    private static long toEpochMonth(Date date) {
        return date == null ? Long.MAX_VALUE : date.toLocalDate().withDayOfMonth(1).toEpochDay();
    }


    @Override
    protected void createData(ContractDao contractDao, HistoryDao historyDao) {
        // Nothing to do
    }

    @Override
    protected void updateDatabase(ContractDao contractDao, HistoryDao historyDao) {

        // 更新対象の電話番号を取得
        Key key = keySelector.get();

        if (skipDatabaseAccess) {
            return;
        }

        // 当該電話番号の契約を取得
        List<Contract> contracts = contractDao.getContracts(key.getPhoneNumber());
        if (contracts.isEmpty()) {
            // 電話番号にマッチする契約がなかったとき(DBに追加される前の電話番号を選んだ場合) => 基本的にありえない
            LOG.warn("No contract found for phoneNumber = {}.", key.getPhoneNumber());
            return;
        }

        // 取得した契約から更新対象の契約を更新する
        Contract updatingContract = getUpdatingContract(contracts, key);
        if (updatingContract == null) {
            // 契約の更新に失敗したとき(DBが壊れている可能性が高い)
            LOG.warn("Fail to create valid update contracts for phone number: {}", contracts.get(0).getPhoneNumber());
            for(Contract c: contracts) {
                LOG.warn("   " + c.toString());
            }
            return;
        }

        // 契約を更新

        int ret =  contractDao.update(updatingContract);

        if (ret != 1 && config.dbmsType != DbmsType.ICEAXE ) {
            // select ～ updateの間に対象レコードが削除されたケース -> 基本的にありえない
            throw new RuntimeException("Fail to update contracts: " + updatingContract);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("ONLINE_APP: Update 1 record from contracts(phoneNumber = {}, startDate = {}, endDate = {}).",
                    updatingContract.getPhoneNumber(), updatingContract.getStartDate(), updatingContract.getEndDate());
        }
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
            contract.setEndDate((Date)null);
        }
    }

    /**
     * 契約終了日を設定する
     *
     */
    class Updater2 implements Updater {
        @Override
        public void update(Contract contract) {
            long startTime = contract.getStartDate().getTime();
            int d = (int) ((config.maxDate.getTime() - startTime) / DAY_IN_MILLS);
            int r = random.nextInt(d + 1);
            contract.setEndDate(new Date(startTime + r * DAY_IN_MILLS));
        }
    }

    @Override
    public TxLabel getTxLabel() {
        return TxLabel.ONLINE_MASTER_UPDATE;
    }

    @Override
    public Table getWritePreserveTable() {
        return Table.CONTRACTS;
    }

    @Override
    protected void afterCommitSuccess() {
        // Nothing to do
    }
}
