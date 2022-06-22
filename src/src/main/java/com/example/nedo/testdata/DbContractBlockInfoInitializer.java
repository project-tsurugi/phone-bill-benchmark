package com.example.nedo.testdata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;

import com.example.nedo.app.Config;
import com.example.nedo.db.old.DBUtils;

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
	void init() throws SQLException {
		// AbstractContractBlockInfoInitializerのフィールドを初期化
		waitingBlocks = new HashSet<Integer>();
		activeBlockNumberHolder = new ActiveBlockNumberHolder();

		// 前処理
		PhoneNumberGenerator phoneNumberGenerator = new PhoneNumberGenerator(config);
		int blockSize = config.getContractBlockSize();

		// Contractsテーブルをフルスキャンしてブロック情報を作成する
		String sql = "select phone_number from contracts order by phone_number;";
		try (Connection conn = DBUtils.getConnection(config);
				PreparedStatement ps = conn.prepareStatement(sql);
				ResultSet rs = ps.executeQuery()) {
			int blockNumber = 0;
			int recordsInBlock = 0;
			long topPhoneNumberOfNextBlock = blockSize;
			while (rs.next()) {
				long phoneNumber = Long.parseLong(rs.getString(1));
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
