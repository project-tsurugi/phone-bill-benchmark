package com.tsurugidb.benchmark.phonebill.app;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
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

	public void execute(List<ConfigInfo> configs) throws Exception {
		throw new RuntimeException("Not suportted.");
	}

	public void execute(String hostname, int port) throws Exception {
		throw new RuntimeException("Not suportted.");
	}


	protected static List<ConfigInfo> createConfigInfos(String[] args, int startPos) throws IOException {
		List<ConfigInfo> configInfos = new ArrayList<>(args.length);
		for (int i = startPos; i < args.length; i++) {
			String arg = args[i];
			ConfigInfo info = new ConfigInfo(arg);
			configInfos.add(info);
		}
		return configInfos;
	}


	protected static class ConfigInfo {
		public ConfigInfo(String pathString) throws IOException {
			this.configPath = Path.of(pathString);
			this.config = Config.getConfig(pathString);
		}
		Path configPath;
		Config config;
	}
}
