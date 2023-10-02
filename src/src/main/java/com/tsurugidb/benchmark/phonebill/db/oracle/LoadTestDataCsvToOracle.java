package com.tsurugidb.benchmark.phonebill.db.oracle;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.Config.DbmsType;
import com.tsurugidb.benchmark.phonebill.app.ExecutableCommand;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.testdata.CsvUtils;

public class LoadTestDataCsvToOracle extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(LoadTestDataCsvToOracle.class);

    public static void main(String[] args) throws Exception {
        Config config = Config.getConfig(args);
        LoadTestDataCsvToOracle loadTestDataCsvToOracle = new LoadTestDataCsvToOracle();
        loadTestDataCsvToOracle.execute(config);
    }

    @Override
    public void execute(Config config) throws Exception {
        if (config.dbmsType != DbmsType.ORACLE_JDBC) {
            LOG.error("This configuration is not for the Oracle.");
        } else {
            PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config);
            Ddl ddl = manager.getDdl();
            ddl.prepareLoadData();
            List<Path> list = createControlFiles(config);
            for (Path path : list) {
                execSqlLoader(config, path);
            }
            ddl.afterLoadData();
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
        Path filename = path.getFileName();
        if (filename == null) {
            throw new IOException("empty path: " + path.toString());
        }
        LOG.info("SQL*Loader start with control file: " + path.toAbsolutePath().toString());
        long startTime = System.currentTimeMillis();


        List<String> cmd = new ArrayList<String>();
        cmd.add(config.oracleSqlLoaderPath);
        String sidPart = (config.oracleSqlLoaderSid == null || config.oracleSqlLoaderSid.isEmpty()) ? ""
                : "@" + config.oracleSqlLoaderSid;
        cmd.add(config.user + "/" + config.password + sidPart);
        cmd.add("control=" + path.toAbsolutePath().toString());

        ProcessBuilder builder = new ProcessBuilder(cmd);
        builder.redirectErrorStream(true);
        builder.directory(new File(config.csvDir));
        Process process = builder.start();
        String charsetName = "\\".equals(System.getProperty("file.separator")) ? "SJIS" : "utf-8";

        try (BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream(), charsetName))) {
            for(;;) {
                String line = br.readLine();
                if (line == null) {
                    break;
                }
                System.out.println(line);
            }
        }


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
     * @throws IOException
     */
    private List<Path> createControlFiles(Config config) throws IOException {
        Path csvDir = Paths.get(config.csvDir);
        Path contractsCsvPath = CsvUtils.getContractsFilePath(csvDir);
        List<Path> historyCsvPathList = CsvUtils.getHistortyFilePaths(csvDir);

        Path contractsCtrlFilePath = csvDir.resolve("contracts.ctl");
        Path historyCtrlFilePath = csvDir.resolve("history.ctl");

        List<Path> list = new ArrayList<>();
        list.add(historyCtrlFilePath);
        list.add(contractsCtrlFilePath);

        try (PrintStream ps = new PrintStream(historyCtrlFilePath.toFile(), "utf-8")) {
            ps.println("OPTIONS (");
            ps.println("        DIRECT = TRUE,");
            ps.println("        MULTITHREADING = TRUE,");
            ps.println("        PARALLEL = TRUE");
            ps.println(")");
            ps.println("LOAD DATA");
            ps.println("CHARACTERSET UTF8");
            for (Path path: historyCsvPathList) {
                ps.println("INFILE '" + path.toAbsolutePath().toString() + "'");
            }
            ps.println("APPEND");
            ps.println("INTO TABLE HISTORY");
            ps.println("FIELDS TERMINATED BY \",\"");
            ps.println("TRAILING NULLCOLS");
            ps.println("(");
            ps.println("  caller_phone_number,");
            ps.println("  recipient_phone_number,");
            ps.println("  payment_category,");
            ps.println("  start_time,");
            ps.println("  time_secs,");
            ps.println("  charge,");
            ps.println("  df");
            ps.println(")");
        }

        try (PrintStream ps = new PrintStream(contractsCtrlFilePath.toFile(), "utf-8")) {
            ps.println("OPTIONS (");
            ps.println("        DIRECT = TRUE,");
            ps.println("        MULTITHREADING = TRUE,");
            ps.println("        PARALLEL = TRUE");
            ps.println(")");
            ps.println("LOAD DATA");
            ps.println("CHARACTERSET UTF8");
            ps.println("INFILE '" + contractsCsvPath.toAbsolutePath().toString() + "'");
            ps.println("APPEND");
            ps.println("INTO TABLE CONTRACTS");
//			ps.println("TIMESTAMP FORMAT \"YY-MM-DD HH24:MI:SS.FF3\"");
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
