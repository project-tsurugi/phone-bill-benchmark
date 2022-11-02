package com.tsurugidb.benchmark.phonebill.app;

import java.util.List;

/**
 * Mainクラスから呼び出される各コマンドの基底クラス.
 * <p>
 * 引数が異なる3つのexecuteメソッドのどれかをを実装する
 */
public abstract class ExecutableCommand {
	public void execute(Config config) throws Exception {
		throw new RuntimeException("Not suportted.");
	}

	public void execute(List<Config> configs) throws Exception {
		throw new RuntimeException("Not suportted.");
	}

	public void execute(String hostname, int port) throws Exception {
		throw new RuntimeException("Not suportted.");
	}
}
