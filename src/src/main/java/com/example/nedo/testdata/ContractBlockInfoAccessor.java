package com.example.nedo.testdata;

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
	public int getNewBlock();

	/**
	 * 引数で指定されたブロック番号のブロックをアクティブなブロックとして通知する、
	 *
	 * @param blockNumber
	 * @return
	 */
	public void submit(int blockNumber);

	/**
	 * アクティブなブロックの情報を返す
	 *
	 * @return
	 */
	public ActiveBlockNumberHolder getActiveBlockInfo();
}
