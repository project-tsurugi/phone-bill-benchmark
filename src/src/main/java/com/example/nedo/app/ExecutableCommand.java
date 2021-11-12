package com.example.nedo.app;

/**
 * Mainクラスから呼び出される各コマンドの基底クラス.
 * <p>
 * 引数が異なる二つのexecuteメソッドのどちらかを実装する
 */
public abstract class ExecutableCommand {
	public void execute(Config config) throws Exception {
		throw new RuntimeException("Not suportted.");
	}

	public void execute(String hostname, int port) throws Exception {
		throw new RuntimeException("Not suportted.");
	}
}
