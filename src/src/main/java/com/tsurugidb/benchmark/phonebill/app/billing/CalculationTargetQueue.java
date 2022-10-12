package com.tsurugidb.benchmark.phonebill.app.billing;

import java.util.Collection;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CalculationTargetを格納するQueue
 */
/**
 *
 */
/**
 *
 */
public class CalculationTargetQueue {
    private static final Logger LOG = LoggerFactory.getLogger(CalculationTargetQueue.class);

	/**
	 * キュー本体
	 */
	private BlockingDeque<CalculationTarget> queue;


	/**
	 * キューに入れたタスクの総数
	 */
	private AtomicInteger totalQueuedTaasks = new AtomicInteger(0);

	/**
	 * キューの状態を表す文字列
	 */
	private volatile String status;

	/**
	 * このキューからタスクを取り出すスレッドの数
	 */
	private int threadCount;



	/**
	 * コンストラクタ
	 */
	public CalculationTargetQueue(int threadCount) {
		this.threadCount = threadCount;
		queue = new LinkedBlockingDeque<>();
		updateStatus();
	}


	private synchronized void updateStatus() {
		status = "Contracts queue status: total queued taasks = " + totalQueuedTaasks + ", tasks in queue = "
				+ queue.size();
	}

	/**
	 * キューの状態を表す文字列を返す
	 */
	public synchronized String getStatus() {
		return status;
	}


	/**
	 * queue.take()の結果を返す
	 *
	 * @return
	 * @throws InterruptedException
	 */
	public CalculationTarget take() {
		for (;;) {
			try {
				CalculationTarget t = queue.take();
				updateStatus();
				return t;
			} catch (InterruptedException e) {
				LOG.debug("InterruptedException caught and continue taking a calculation target", e);
			}
		}
	}

	/**
	 * queueにtargetを追加する。InterruptedException発生時は成功するまでリトライする。
	 *
	 * @param target
	 */
	public void put(CalculationTarget target) {
		for(;;) {
			try {
				queue.put(target);
				break;
			} catch (InterruptedException e) {
				LOG.debug("InterruptedException caught and continue puting a calculation target", e);
			}
		}
		totalQueuedTaasks.incrementAndGet();
		updateStatus();
	}


	/**
	 * 指定のコレクションの全要素をqueueの先頭に追加する。
	 *
	 * @param c
	 */
	public void putFirst(Collection<CalculationTarget> c) {
		for(CalculationTarget target: c) {
			for(;;) {
				try {
					queue.putFirst(target);
					break;
				} catch (InterruptedException e) {
					LOG.debug("InterruptedException caught and continue puting a calculation target", e);
				}
			}
		}
	}


	/**
	 * これ以上Queueに入れるタスクがないことを宣言する。
	 *
	 * inputClosedフラグを立て、Queueを読むスレッドの数だけ、
	 * EndOfTaskをキューに入れる。
	 */
	public void setEndOfTask (){
		// EndOfTaskをキューに入れる
		for (int i =0; i < threadCount; i++) {
			put(CalculationTarget.getEndOfTask());
		}
		updateStatus();
	}

	/**
	 * キューに残っているタスクをクリアする
	 */
	public void clear() {
		queue.clear();
	}


	/**
	 * Queueに残っている要素数を返す(UT用)
	 *
	 * @return
	 */
	public int size() {
		return queue.size();
	}
}
