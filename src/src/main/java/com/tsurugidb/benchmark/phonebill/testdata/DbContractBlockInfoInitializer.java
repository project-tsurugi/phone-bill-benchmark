package com.tsurugidb.benchmark.phonebill.testdata;

import java.util.HashSet;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;

/**
 * データベースを読み取り、契約マスタのブロック情報を初期化する
 *
 */
public class DbContractBlockInfoInitializer extends AbstractContractBlockInfoInitializer {
    private Config config;


    public DbContractBlockInfoInitializer(Config config) {
        this.config = config;
    }

    @Override
    void init() {
        // AbstractContractBlockInfoInitializerのフィールドを初期化
        waitingBlocks = new HashSet<Integer>();
        activeBlockNumberHolder = new ActiveBlockNumberHolder();

        // 前処理
        PhoneNumberGenerator phoneNumberGenerator = new PhoneNumberGenerator(config);
        int blockSize = ContractInfoReader.getContractBlockSize(config);

        // Contractsテーブルをフルスキャンしてブロック情報を作成する
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            ContractDao dao = manager.getContractDao();
            List<String> phoneNumbers = manager.execute(TxOption.ofRTX(3,TxLabel.INITIALIZE), () -> dao.getAllPhoneNumbers());

            int blockNumber = 0;
            int recordsInBlock = 0;
            long topPhoneNumberOfNextBlock = blockSize;
            for (String s : phoneNumbers) {
                long phoneNumber = Long.parseLong(s);
                while (phoneNumber >= topPhoneNumberOfNextBlock) {
                    checkBlockFilled(blockSize, blockNumber, recordsInBlock);
                    blockNumber++;
                    recordsInBlock = 0;
                    topPhoneNumberOfNextBlock = phoneNumberGenerator
                            .getPhoneNumberAsLong((blockNumber + 1) * blockSize);
                }
                recordsInBlock++;
            }
            checkBlockFilled(blockSize, blockNumber, recordsInBlock);
            numberOfBlocks = blockNumber + 1;
        }
    }

    /**
     * ブロックが満たされているかを確認しフィールドを更新する。
     *
     * @param blockSize
     * @param blockNumber
     * @param recordsInBlock
     */
    private void checkBlockFilled(int blockSize, int blockNumber, int recordsInBlock) {
        if (recordsInBlock == blockSize) {
            activeBlockNumberHolder.addActiveBlockNumber(blockNumber);
        } else {
            waitingBlocks.add(blockNumber);
        }
    }
}
