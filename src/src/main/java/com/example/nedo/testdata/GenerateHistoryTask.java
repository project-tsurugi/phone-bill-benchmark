package com.example.nedo.testdata;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.db.Duration;
import com.example.nedo.db.History;
import com.example.nedo.testdata.TestDataGenerator.HistoryWriter;

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
	 * 同一のキーのデータを作らないために作成済みのHistoryDataのKeyを記録するSet
	 */
	private Set<Key> keySet;

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
	 * 通話開始時刻の最小値
	 */
	private long start;

	/**
	 * 通話開始時刻の最大値+1
	 */
	private long end;


	/**
	 * 生成する通話履歴数
	 */
	private long numbeOfHistory;


	/**
	 * 通話履歴の出力先
	 */
	private HistoryWriter historyWriter;


	/**
	 * 契約内容を取得するためのクラス
	 */
	private ContractReader contractReader;

	/**
	 * 契約期間
	 */
	private List<Duration> durationList;


	/**
	 * 契約期間のパターンを記録するリスト
	 */
	private Config config;

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
		random = params.random;
		phoneNumberGenerator = params.phoneNumberGenerator;
		start = params.start;
		end = params.end;
		numbeOfHistory = params.numberOfHistory;
		historyWriter=params.historyWriter;
		config = params.config;
		durationList = params.durationList;
		contractReader = params.contractReader;
	}

	@Override
	public void run() {
		LOG.debug("start task id = " + taskId);
		try {
			init();
			for (long i = 0; i < numbeOfHistory; i++) {
				History h = createHistoryRecord();
				historyWriter.write(h);
			}
			historyWriter.cleanup();
		} catch (IOException | SQLException e) {
			// RunnnableでなくCallableを実装し終了時様態を返すようにする
			e.printStackTrace();
		}
		LOG.debug("end task id = " + taskId);
	}

	public void init() throws IOException {
		historyWriter.init();
		keySet = new HashSet<Key>((int)numbeOfHistory);
		callerPhoneNumberSelector = PhoneNumberSelector.createSelector(random,
				config.callerPhoneNumberDistribution,
				config.callerPhoneNumberScale,
				config.callerPhoneNumberShape, contractReader, durationList.size());
		recipientPhoneNumberSelector = PhoneNumberSelector.createSelector(random,
				config.recipientPhoneNumberDistribution,
				config.recipientPhoneNumberScale,
				config.recipientPhoneNumberShape, contractReader, durationList.size());
		callTimeGenerator = CallTimeGenerator.createCallTimeGenerator(random, config);
	}

	/**
	 * 通話履歴を生成する
	 *
	 * @param targetDuration
	 * @return
	 */
	private History createHistoryRecord() {
		// 重複しないキーを選ぶ
		Key key = new Key();
		int counter = 0;
		for (;;) {
			key.startTime = TestDataUtils.getRandomLong(random, start, end);
			key.callerPhoneNumber = callerPhoneNumberSelector.selectPhoneNumber(key.startTime, -1);
			if (keySet.contains(key)) {
				if (++counter > 5) {
					LOG.info("A duplicate key was found, so another key will be created.(key = {}, KeySetSize = {} ", key, keySet.size());
					counter = 0;
				}
			} else {
				keySet.add(key);
				return createHistoryRecord(key);
			}
		}
	}

	/**
	 * 指定のキーを持つ通話履歴を生成する
	 *
	 * @param key
	 * @return
	 */
	private History createHistoryRecord(Key key) {
		History history = new History();
		history.startTime = new Timestamp(key.startTime);

		// 電話番号の生成
		long r = recipientPhoneNumberSelector.selectPhoneNumber(key.startTime, key.callerPhoneNumber);
		history.callerPhoneNumber = phoneNumberGenerator.getPhoneNumber(key.callerPhoneNumber);
		history.recipientPhoneNumber = phoneNumberGenerator.getPhoneNumber(r);

		// 料金区分(発信者負担、受信社負担)
		// TODO 割合を指定可能にする
		history.paymentCategorty = random.nextInt(2) == 0 ? "C" : "R";

		// 通話時間
		history.timeSecs = callTimeGenerator.getTimeSecs();
		return history;
	}


	/**
	 * 指定の通話開始時刻の通話履歴を生成する
	 *
	 * @param startTime
	 * @return
	 */
	public History createHistoryRecord(long startTime) {
		Key key = new Key();
		key.startTime = startTime;
		key.callerPhoneNumber = callerPhoneNumberSelector.selectPhoneNumber(startTime, -1);
		return createHistoryRecord(key);
	}

	/**
	 * タスクのパラメータ
	 *
	 */
	public static class Params implements Cloneable {
		int taskId;
		Config config;
		Random random;
		ContractReader contractReader;
		PhoneNumberGenerator phoneNumberGenerator;
		List<Duration> durationList;
		long start;
		long end;
		long numberOfHistory;
		public HistoryWriter historyWriter;

		@Override
		public Params clone() {
			try {
				return (Params) super.clone();
			} catch (CloneNotSupportedException e) {
				throw new InternalError(e.toString());
			}
		}
	}


	private static class Key {
		long startTime;
		long callerPhoneNumber;

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (callerPhoneNumber ^ (callerPhoneNumber >>> 32));
			result = prime * result + (int) (startTime ^ (startTime >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Key other = (Key) obj;
			if (callerPhoneNumber != other.callerPhoneNumber)
				return false;
			if (startTime != other.startTime)
				return false;
			return true;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Key [startTime=");
			builder.append(startTime);
			builder.append(", callerPhoneNumber=");
			builder.append(callerPhoneNumber);
			builder.append("]");
			return builder.toString();
		}
	}
}
