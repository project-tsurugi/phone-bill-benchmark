package com.tsurugidb.benchmark.phonebill.online;

import java.io.IOException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption.Table;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;

/**
 * 指定の頻度で、契約マスタのレコードをdelete/insertするアプリケーション
 *
 */
public class MasterDeleteInsertApp extends AbstractOnlineApp {
    private static final Logger LOG = LoggerFactory.getLogger(MasterDeleteInsertApp.class);

    private RandomKeySelector<Key> keySelector;

    /**
     * 削除したレコード、次のTXでこのレコードをインサートする
     */
    private Contract deletedContract = null;

    /**
     * 削除しようとしてるレコード
     */
    private Contract deletingContact;


    public MasterDeleteInsertApp(Config config, Random random, RandomKeySelector<Key> keySelector) throws IOException {
        super(config.masterDeleteInsertRecordsPerMin, config, random);
        this.keySelector = keySelector;
    }

    @Override
    protected void createData(ContractDao contractDao, HistoryDao historyDao) {
        // Nothing to do
    }

    @Override
    protected void updateDatabase(ContractDao contractDao, HistoryDao historyDao) {
        if (deletedContract == null) {
            delete(contractDao, historyDao);
        } else {
            insert(contractDao, historyDao);
        }
    }


    private void insert(ContractDao contractDao, HistoryDao historyDao) {
        int ret = contractDao.insert(deletedContract);
        LOG.debug("ONLINE_APP: Insert {} record to contracts(phoneNumber = {}, startDate = {}).", ret,
                deletedContract.getPhoneNumber(), deletedContract.getStartDate());
    }

    private void delete(ContractDao contractDao, HistoryDao historyDao) {
        Key key = keySelector.getAndRemove();
        deletingContact = contractDao.getContract(key);
        int ret = contractDao.delete(key);
        LOG.debug("ONLINE_APP: Delete {} record from  contracts(phoneNumber = {}, startDate = {}).", ret,
                key.getPhoneNumber(), key.getStartDate());
    }

    @Override
    public TxLabel getTxLabel() {
        if (deletedContract == null) {
            return TxLabel.ONLINE_MASTER_DELETE;
        } else {
            return TxLabel.ONLINE_MASTER_INSERT;
        }
    }

    @Override
    public Table getWritePreserveTable() {
        return Table.CONTRACTS;
    }

    @Override
    protected void afterCommitSuccess() {
        if (deletedContract == null) {
            deletedContract = deletingContact;
        } else {
            keySelector.add(deletingContact.getKey());
            deletedContract = null;
        }
    }
}
