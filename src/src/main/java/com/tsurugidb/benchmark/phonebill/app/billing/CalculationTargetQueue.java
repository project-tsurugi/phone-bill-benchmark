package com.tsurugidb.benchmark.phonebill.app.billing;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;

/**
 * CalculationTargetを格納するクラス。コンストラクタでTargetのコレクションを受け取り、
 * 各CalcurationTaskのCalculationTargetを配布、すべてのCalculationTargetの処理終了後に
 * 処理の終了を伝える役割を持つ。
 *
 */
public class CalculationTargetQueue {
	/**
	 * CalculationTargetを保持するQueue
	 */
	private Queue<CalculationTarget> queue;

	/**
	 * 処理中の数
	 */
	private int numberOfRunningTargets;

	/**
	 * 処理対象の数
	 */
	private int numberOfTargts;


	/**
	 * 処理終了を表すフラグ
	 */
	private volatile boolean finished;


	/**
	 * Queueのステータスを表す文字列
	 */
	String status;


	/**
	 * コンストラクタ
	 */
	public CalculationTargetQueue(Collection<CalculationTarget> targets) {
		queue = new ArrayDeque<>(targets);
		numberOfTargts = queue.size();
		numberOfRunningTargets = 0;
		finished = false;
		updateSatus();
	}



	private void updateSatus() {
		status = "Contracts queue status: total size = " + numberOfTargts + ", in queue = "
				+ queue.size() + ", running = " + numberOfRunningTargets;
	}


	/**
	 * キューの状態を表す文字列を返す
	 */
	public String getStatus() {
		return status;
	}


	/**
	 * Queueから処理対象を取り出す。queueが空で処理中が0の場合はnullを返す。
	 * 処理対象を取得できるかnullを返す状況になるまでこのメソッドはブロックする。
	 *
	 * @return
	 * @throws InterruptedException
	 */
	public CalculationTarget take() throws InterruptedException {
		for (;;) {
			CalculationTarget target = poll();
			if (target != null || finished) {
				return target;
			}
			Thread.sleep(10);
		}
	}

	/**
	 * queueから計算対象を取り出し、カウンタとフラグを更新する。
	 *
	 * @return
	 */
	public synchronized CalculationTarget poll() {
		CalculationTarget target = queue.poll();
		if (target != null) {
			numberOfRunningTargets++;
			updateSatus();
		}
		if (numberOfRunningTargets <= 0) {
			finished = true;
		}
		return target;
	}


	/**
	 * 処理に失敗したCalcurationTargetをqueuenに戻す
	 */
	public synchronized void revert(Collection<CalculationTarget> targets) {
		queue.addAll(targets);
		numberOfRunningTargets -= targets.size();
		updateSatus();
	}


	/**
	 * 処理に失敗したCalcurationTargetをqueuenに戻す
	 */
	public synchronized void revert(CalculationTarget target) {
		queue.add(target);
		numberOfRunningTargets--;
		updateSatus();
	}


	/**
	 * 処理に成功したCalclationTargetをセットする
	 */
	public synchronized void success(Collection<CalculationTarget> targets) {
		numberOfRunningTargets -= targets.size();
		updateSatus();
	}

	/**
	 * 処理に成功したCalclationTargetをセットする
	 */
	public synchronized void success(CalculationTarget target) {
		numberOfRunningTargets --;
		updateSatus();
	}


	/**
	 * これ以上queueに処理対象が残っていないかを調べる
	 *
	 * @return 処理対象が残っていないときtrue
	 */
	public boolean finished() {
		return finished;
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
