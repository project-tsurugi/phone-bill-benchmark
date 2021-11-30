package com.example.nedo.app.billing;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CalculationTargetを格納するQueue
 */
public class CalculationTargetQueue {
    private static final Logger LOG = LoggerFactory.getLogger(CalculationTargetQueue.class);

	/**
	 * キュー本体
	 */
	private BlockingQueue<CalculationTarget> queue;


	/**
	 * キューに入れたタスクの総数
	 */
	private AtomicInteger totalQueuedTaasks = new AtomicInteger(0);


	/**
	 * 入力が終了したことを示すフラグ
	 */
	private boolean inputClosed = false;


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
		queue = new LinkedBlockingQueue<CalculationTarget>();
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
	public CalculationTarget take() throws InterruptedException {
		CalculationTarget t = queue.take();
		updateStatus();
		return t;
	}


	/**
	 * queueにtargetを追加する。InterruptedException発生時は成功するまでリトライする。
	 *
	 * @param target
	 */
	public void put(CalculationTarget target) {
		if (inputClosed) {
			throw new IllegalStateException("Input was closed.");
		}
		for(;;) {
			try {
				queue.put(target);
				break;
			} catch (InterruptedException e) {
				LOG.debug("InterruptedException caught and continue puting calculation_target", e);
			}
		}
		totalQueuedTaasks.incrementAndGet();
		updateStatus();
	}


	/**
	 * これ以上Queueに入れるタスクがないことを宣言する。
	 *
	 * inputClosedフラグを立て、Queueを読むスレッドの数だけ、
	 * EndOfTaskをキューに入れる。
	 */
	public void setInputClosed (){
		// EndOfTaskをキューに入れる
		for (int i =0; i < threadCount; i++) {
			put(CalculationTarget.getEndOfTask());
		}
		inputClosed = true;
		updateStatus();
	}


	/**
	 * Queueを空にして、setInputClosed()を呼び出す
	 */
	public void abort() {
		queue.clear();
		setInputClosed();
	}

	/**
	 * キューに残っているタスクをクリアする
	 */
	public void clear() {
		queue.clear();
	}
}
