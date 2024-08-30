/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
