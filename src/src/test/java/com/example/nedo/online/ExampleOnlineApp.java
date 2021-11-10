package com.example.nedo.online;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * AbstractOnlineAppのスケジューラの動作確認用サンプルアプリ.
 *
 */
public class ExampleOnlineApp extends AbstractOnlineApp {
    private static final Logger LOG = LoggerFactory.getLogger(ExampleOnlineApp.class);
	private final DateFormat DF = new SimpleDateFormat("HH:mm:ss.SSS");

	public ExampleOnlineApp() throws SQLException, IOException {
		super(20, Config.getConfig(), new Random());
	}

	@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
	public static void main(String[] args) throws InterruptedException, SQLException, IOException {
		ExampleOnlineApp app = new ExampleOnlineApp();
		ExecutorService service = Executors.newSingleThreadExecutor();
		service.submit(app);
		Thread.sleep(60 * 1000 * 5); // 5分実行して終了する。
		app.terminate();
		service.shutdown();
		service.awaitTermination(1, TimeUnit.DAYS);
	}

	@Override
	protected void atScheduleListCreated(List<Long> scheduleList) {
		for(long schedule: scheduleList) {
			LOG.info("Scheduled at {}", DF.format(new Date(schedule)));
		}
	}

	@Override
	protected void createData() {
		LOG.info("createData called.");
	}

	@Override
	protected void updateDatabase() {
		LOG.info("updateDatabase called.");
	}
}
