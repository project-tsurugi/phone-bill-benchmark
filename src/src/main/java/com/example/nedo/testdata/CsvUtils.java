package com.example.nedo.testdata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CsvUtils {
	static final String HISTORY_REGEXP ="history-[0-9]+\\.csv";


	/**
	 * 通話履歴のCSVファイルのパスを取得する
	 *
	 * @param dir CSVファイルのディレクトリ
	 * @return
	 * @throws IOException
	 */
	public static List<Path> getHistortyFilePaths(Path dir) throws IOException {
		return Files.list(dir).filter(p -> Pattern.matches(HISTORY_REGEXP, p.getFileName().toString())).sorted()
				.collect(Collectors.toList());
	}

	/**
	 * 契約のCSVSファイルのパスを取得する
	 *
	 * @param dir CSVファイルのディレクトリ
	 * @return
	 */
	public static Path getContractsFilePath(Path dir) {
		return dir.resolve("contracts.csv");
	}


	/**
	 * n番目の通話履歴のCSVファイルのパスを取得する
	 *
	 * @param n
	 * @param dir CSVファイルのディレクトリ
	 * @return
	 */
	public static Path getHistortyFilePath(Path dir, int n) {
		return  dir.resolve("history-" + n +  ".csv");
	}
}
