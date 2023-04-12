package com.tsurugidb.benchmark.phonebill.testdata;

import java.io.IOException;

/**
 * 契約のブロックに関する情報にアクセスするためのアクセサ
 *
 */
public interface ContractBlockInfoAccessor {
	/**
	 * 契約を生成するためのブロックのブロック番号を取得する
	 *
	 * @return
	 */
	public int getNewBlock() throws IOException;

	/**
	 * 引数で指定されたブロック番号のブロックをアクティブなブロックとして通知する、
	 *
	 * @param blockNumber
	 * @return
	 * @throws IOException
	 */
	public void submit(int blockNumber) throws IOException;

	/**
	 * アクティブなブロックの情報を返す
	 *
	 * @return
	 */
	public ActiveBlockNumberHolder getActiveBlockInfo() throws IOException;
}
