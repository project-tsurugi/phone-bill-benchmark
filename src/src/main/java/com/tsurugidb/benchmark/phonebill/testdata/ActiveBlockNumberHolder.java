package com.tsurugidb.benchmark.phonebill.testdata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * アクティブなブロック番号の情報を保持するクラス
 * <p>
 * 0から始まるアクティブなブロックのブロック番号を保持する。アクティブなブロックとは、
 * 契約レコードのブロック全体にレコードが存在し、 * 任意のスレッドからアクセス可能な
 * ブロックである。
 *
 */
public class ActiveBlockNumberHolder implements Cloneable {
	/**
	 * アクティブなブロック数
	 */
	private int numberOfActiveBlacks = 0;

	/**
	 * 先頭の連続したアクティブなブロックのブロック番号の最大値、最初の連続したアクティブなブロックが存在しない場合は-1
	 */
	private int maximumBlockNumberOfFirstConsecutiveActiveBlock = -1;


	/**
	 * 最初の連続したアクティブなブロックを除く、アクティブなブロックのリスト
	 */
	private List<Integer> activeBlocks = new ArrayList<Integer>();


	/**
	 * 指定されたブロック番号のリストを本メソッドが想定するリストに変換してセットする。
	 *
	 * @param activeBlocks セットする activeBlocks
	 */
	public void setActiveBlocks(List<Integer> list) {
		maximumBlockNumberOfFirstConsecutiveActiveBlock = -1;
		numberOfActiveBlacks = list.size();

		activeBlocks.clear();
		activeBlocks.addAll(list);
		if (activeBlocks.isEmpty()) {
			return;
		}
		setMaximumBlockNumberOfFirstConsecutiveActiveBlock();
	}

	/**
	 * 先頭の連続したアクティブなブロックのブロック番号の最大値をセットする
	 */
	private void setMaximumBlockNumberOfFirstConsecutiveActiveBlock() {
		int offset = maximumBlockNumberOfFirstConsecutiveActiveBlock + 1;
		Collections.sort(activeBlocks);
		int i;
		for (i = 0; i < activeBlocks.size(); i++) {
			if (activeBlocks.get(i) != i + offset) {
				break;
			}
		}
		if (i < 1) {
			return;
		}
		maximumBlockNumberOfFirstConsecutiveActiveBlock = offset +  i - 1;
		activeBlocks = activeBlocks.subList(i, activeBlocks.size());
	}




	public int getMaximumBlockNumberOfFirstConsecutiveActiveBlock() {
		return maximumBlockNumberOfFirstConsecutiveActiveBlock;
	}


	public int getNumberOfActiveBlacks() {
		return numberOfActiveBlacks;
	}


	/**
	 * アクティブなブロック番号を追加する
	 *
	 * @param n
	 */
	public void addActiveBlockNumber(int n) {
		// 不正な値と、既に存在する値をチェック
		if (n < 0) {
			throw new IllegalArgumentException("Negative value: " + n );
		}
		if (n <= maximumBlockNumberOfFirstConsecutiveActiveBlock || activeBlocks.contains(n)) {
			throw new IllegalArgumentException("Already active block: " + n );
		}

		numberOfActiveBlacks++;;

		activeBlocks.add(n);
		setMaximumBlockNumberOfFirstConsecutiveActiveBlock();
	}


	public List<Integer> getActiveBlocks() {
		return Collections.unmodifiableList(activeBlocks);
	}

	@Override
	public ActiveBlockNumberHolder clone()  {
		try {
			return (ActiveBlockNumberHolder) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new InternalError(e);
		}
	}

	/**
	 * ActiveBlockNumberHolderのインスタンスを表す文字列を作成する
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(numberOfActiveBlacks);
		sb.append(',');
		sb.append(maximumBlockNumberOfFirstConsecutiveActiveBlock);
		for(int i: activeBlocks) {
			sb.append(',');
			sb.append(i);
		}
		return sb.toString();
	}

	/**
	 * ActiveBlockNumberHolderのインスタンスを表す文字列から、ActiveBlockNumberHolderのインスタンスを作成する
	 *
	 * @return
	 */
	public static ActiveBlockNumberHolder valueOf(String str) {
		ActiveBlockNumberHolder holder = new ActiveBlockNumberHolder();
		String[] strs = str.split(",");
		holder.numberOfActiveBlacks = Integer.parseInt(strs[0]);
		holder.maximumBlockNumberOfFirstConsecutiveActiveBlock = Integer.parseInt(strs[1]);
		for(int i = 2; i < strs.length; i++) {
			holder.activeBlocks.add(Integer.parseInt(strs[i]));
		}
		return holder;
	}
}
