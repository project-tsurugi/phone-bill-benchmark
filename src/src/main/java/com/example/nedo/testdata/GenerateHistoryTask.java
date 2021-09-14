package com.example.nedo.testdata;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.db.Duration;
import com.example.nedo.db.History;

/**
 * 履歴データを作成するタスク
 *
 */
public class GenerateHistoryTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(GenerateHistoryTask.class);


	/*
	 * Task ID
	 */
	private int taskId;

	/**
	 * 同じ発信時刻のデータを作らないための作成済みのHistoryDataの発信時刻を記録するSet
	 */
	private Set<Long> startTimeSet;

	/**
	 * 乱数生成器
	 */
	private Random random;


	/**
	 * 発信者電話番号のSelector
	 */
	private PhoneNumberSelector callerPhoneNumberSelector;

	/**
	 * 受信者電話番号のSelector
	 */
	private PhoneNumberSelector recipientPhoneNumberSelector;

	/**
	 * 通話時間生成器
	 */
	private CallTimeGenerator callTimeGenerator;

	/**
	 * 電話番号生成器
	 */
	private PhoneNumberGenerator phoneNumberGenerator;

	/**
	 * 生成した利通話履歴データを書き込むキュー
	 */
	private BlockingQueue<List<History>> queue;


	/**
	 * 通話開始時刻の最小値
	 */
	private long start;

	/**
	 * 通話開始時刻の最大値+1
	 */
	private long end;

	/**
	 * 1度にキューに書き込む履歴数の最大値
	 */
	private int writeSize;


	/**
	 * 生成する通話履歴数
	 */
	private int numbeOfHistory;


	/**
	 * コンストラクタ.
	 * <br>
	 * run()が呼ばれると、queueに通話開始時刻がstart以上、end未満の通話履歴データをn個キューに書き込む。
	 *
	 * @param config
	 * @param random
	 * @param contractReader
	 * @param phoneNumberGenerator
	 * @param durationList
	 * @param start
	 * @param end
	 * @param writeSize
	 * @param n
	 */
	public GenerateHistoryTask(Params params) {
		taskId = params.taskId;
		startTimeSet = new HashSet<Long>(params.config.numberOfHistoryRecords);
		random = params.random;
		phoneNumberGenerator = params.phoneNumberGenerator;
		callTimeGenerator = CallTimeGenerator.createCallTimeGenerator(random, params.config);
		callerPhoneNumberSelector = PhoneNumberSelector.createSelector(random,
				params.config.callerPhoneNumberDistribution,
				params.config.callerPhoneNumberScale,
				params.config.callerPhoneNumberShape, params.contractReader, params.durationList.size());
		recipientPhoneNumberSelector = PhoneNumberSelector.createSelector(random,
				params.config.recipientPhoneNumberDistribution,
				params.config.recipientPhoneNumberScale,
				params.config.recipientPhoneNumberShape, params.contractReader, params.durationList.size());
		start = params.start;
		end = params.end;
		writeSize = params.writeSize;
		numbeOfHistory = params.numbeOfHistory;
		queue = params.queue;
	}

	@Override
	public void run() {
		LOG.debug("start task id = " + taskId);
		List<History> list = null;
		for (int i = 0; i< numbeOfHistory; i++) {
			if (list == null) {
				list = new ArrayList<History>(writeSize);
			}
			list.add(createHistoryRecord());
			if (list.size() >= writeSize) {
				putToQueue(list);
				list = null;
			}
		}
		putToQueue(list);
		// すべての履歴データの書き込み完了の印として空のリストを書き出す
		putToQueue(Collections.emptyList());
		LOG.debug("end task id = " + taskId);
	}


	/**
	 * 履歴をキューに書き込む(割り込み発生時に無限リトライする)
	 *
	 * @param list
	 */
	private void putToQueue(List<History> list) {
		for (;;) {
			try {
				queue.put(list);
				break;
			} catch (InterruptedException e) {
				// Nothing to do
			}
		}
	}


	/**
	 * 通話開始時刻が指定の範囲に収まる通話履歴を生成する
	 *
	 * @param targetDuration
	 * @return
	 */
	private History createHistoryRecord() {
		// 通話開始時刻
		long startTime;
		do {
			startTime = TestDataUtils.getRandomLong(random, start, end);
		} while (startTimeSet.contains(startTime));
		startTimeSet.add(startTime);
		return createHistoryRecord(startTime);
	}


	/**
	 * 指定の通話開始時刻の通話履歴を生成する
	 *
	 * @param startTime
	 * @return
	 */
	public History createHistoryRecord(long startTime) {
		History history = new History();
		history.startTime = new Timestamp(startTime);

		// 電話番号の生成
		long c = callerPhoneNumberSelector.selectPhoneNumber(startTime, -1);
		long r = recipientPhoneNumberSelector.selectPhoneNumber(startTime, c);
		history.callerPhoneNumber = phoneNumberGenerator.getPhoneNumber(c);
		history.recipientPhoneNumber = phoneNumberGenerator.getPhoneNumber(r);

		// 料金区分(発信者負担、受信社負担)
		// TODO 割合を指定可能にする
		history.paymentCategorty = random.nextInt(2) == 0 ? "C" : "R";

		// 通話時間
		history.timeSecs = callTimeGenerator.getTimeSecs();

		return history;
	}

	/**
	 * タスクのパラメータ
	 *
	 */
	static class Params implements Cloneable {
		int taskId;
		Config config;
		Random random;
		ContractReader contractReader;
		PhoneNumberGenerator phoneNumberGenerator;
		List<Duration> durationList;
		long start;
		long end;
		int writeSize;
		int numbeOfHistory;
		BlockingQueue<List<History>> queue;

		@Override
		public Params clone() {
			try {
				return (Params) super.clone();
			} catch (CloneNotSupportedException e) {
				throw new InternalError(e.toString());
			}
		}
	}
}
