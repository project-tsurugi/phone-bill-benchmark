package com.example.nedo.testdata;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.app.CreateTable;
import com.example.nedo.app.ExecutableCommand;
import com.example.nedo.db.DBUtils;

public class LoadTestDataCsvToOracle implements ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(LoadTestDataCsvToOracle.class);

    public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args);
		LoadTestDataCsvToOracle loadTestDataCsvToOracle = new LoadTestDataCsvToOracle();
		loadTestDataCsvToOracle.execute(config);
	}


	@Override
	public void execute(Config config) throws Exception {

		try (Connection conn = DBUtils.getConnection(config);
				Statement stmt = conn.createStatement()) {
			CreateTable.prepareLoadData(stmt, config);
			List<Path> list = createControlFiles(config);
			for(Path path: list) {
	 			execSqlLoader(config, path);
			}
			CreateTable.afterLoadData(stmt, config);
		}
	}

	/**
	 * SQLLoaderを実行する
	 *
	 * @param config
	 * @param path コントロールファイルのパス
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void execSqlLoader(Config config, Path path) throws IOException, InterruptedException {
		String baseFilename = path.getFileName().toString().replaceAll("\\.[^.]*$", "");
		File stdout = Paths.get(config.csvDir).resolve(baseFilename + ".stdout").toFile();
		File stderr = Paths.get(config.csvDir).resolve(baseFilename + ".stderrt").toFile();

		LOG.info("SQL*Loader start with control file: " + path.toAbsolutePath().toString());
		long startTime = System.currentTimeMillis();


		List<String> cmd = new ArrayList<String>();
		cmd.add(config.oracleSqlLoaderPath);
		String sidPart = (config.oracleSqlLoaderSid == null || config.oracleSqlLoaderSid.isEmpty()) ? ""
				: "@" + config.oracleSqlLoaderSid;
		cmd.add(config.user + "/" + config.password + sidPart);
		cmd.add("control=" + path.toAbsolutePath().toString());

		ProcessBuilder builder = new ProcessBuilder(cmd);
		builder.redirectError(stderr);
		builder.redirectOutput(stdout);
		Process process = builder.start();
		process.waitFor();
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "SQL*Loader end in %,.3f sec ";
		LOG.info(String.format(format, elapsedTime / 1000d));

	}


	/**
	 * SQL*Loader用のコントロールファイルを生成する
	 *
	 * @param config
	 * @return 生成したコントロールファイルのパスのリスト
	 * @throws FileNotFoundException
	 */
	private List<Path> createControlFiles(Config config) throws FileNotFoundException {
		Path csvDir = Paths.get(config.csvDir);
		Path historyCtrlFilePath = csvDir.resolve("history.ctl");
		Path contractsCtrlFilePath = csvDir.resolve("contracts.ctl");
		List<Path> list = new ArrayList<>();
		list.add(historyCtrlFilePath);
		list.add(contractsCtrlFilePath);

		try (PrintStream ps = new PrintStream(historyCtrlFilePath.toFile())) {
			ps.println("OPTIONS (");
			ps.println("        DIRECT = TRUE,");
			ps.println("        MULTITHREADING = TRUE,");
			ps.println("        PARALLEL = TRUE");
			ps.println(")");
			ps.println("LOAD DATA");
			ps.println("CHARACTERSET UTF8");
			ps.println("INFILE '" + csvDir.resolve("history.csv").toAbsolutePath().toString() + "'");
			ps.println("BADFILE '"+ csvDir.resolve("history.bad").toAbsolutePath().toString() + "'");
			ps.println("APPEND");
			ps.println("INTO TABLE HISTORY");
			ps.println("FIELDS TERMINATED BY \",\"");
			ps.println("TRAILING NULLCOLS");
			ps.println("(");
			ps.println("  caller_phone_number,");
			ps.println("  recipient_phone_number,");
			ps.println("  payment_categorty,");
			ps.println("  start_time,");
			ps.println("  time_secs,");
			ps.println("  charge,");
			ps.println("  df");
			ps.println(")");
		}

		try (PrintStream ps = new PrintStream(contractsCtrlFilePath.toFile())) {
			ps.println("OPTIONS (");
			ps.println("        DIRECT = TRUE,");
			ps.println("        MULTITHREADING = TRUE,");
			ps.println("        PARALLEL = TRUE");
			ps.println(")");
			ps.println("LOAD DATA");
			ps.println("CHARACTERSET UTF8");
			ps.println("INFILE '" + csvDir.resolve("contracts.csv").toAbsolutePath().toString() + "'");
			ps.println("BADFILE '"+ csvDir.resolve("contracts.bad").toAbsolutePath().toString() + "'");
			ps.println("APPEND");
			ps.println("INTO TABLE CONTRACTS");
			ps.println("FIELDS TERMINATED BY \",\"");
			ps.println("TRAILING NULLCOLS");
			ps.println("(");
			ps.println("  phone_number,");
			ps.println("  start_date,");
			ps.println("  end_date,");
			ps.println("  charge_rule");
			ps.println(")");
		}


		return list;
	}

}
