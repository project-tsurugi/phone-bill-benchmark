package com.tsurugidb.benchmark.phonebill.online;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.annotation.Nonnull;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * @param <T>
 */
public class RandomKeySelector<T> {
	static final String ERROR_NULL_ELEMENTS = "Invalid argument: he 'keys' collection cannot have null elements.";
	static final String ERROR_DUPLICATE_ELEMENTS =  "Invalid argument: The 'keys' collection have duplicate element: ";

	/**
	 * atLeastOnce用のKeyList
	 */
	List<T> aloKeyList;

	/**
	 * 通常のKeyList
	 */
	List<T> keyList;

	/**
	 * 指定のKeyがリストの何番目にあるかを管理するMap
	 */
	Map<T, KeyPosition> keyPositionMap = new HashMap<>();


	/**
	 * RandomKeySelectorが使用する乱数生成器
	 */
	Random random;

	/**
	 * 指定のKeyを削除する
	 *
	 * @param key
	 * @return 指定されたKeyを保持していた場合はtrue
	 */
	public synchronized boolean remove(@Nonnull T key) {
		KeyPosition kp = keyPositionMap.get(key);
		if (kp == null) {
			return false;
		}
		keyPositionMap.remove(key);
		// aloKeyListリストの最後のKeyと指定のKeyを入れ換え、指定のkeyを削除する
		if (kp.aloKeyPostion >= 0) {
			T lastAloKey = getAndRemoveLast(aloKeyList);
			if (lastAloKey != key) {
				aloKeyList.set(kp.aloKeyPostion, lastAloKey);
				keyPositionMap.get(lastAloKey).aloKeyPostion = kp.aloKeyPostion;
			}
		}
		// keyListリストの最後のKeyと指定のKeyを入れ換え、指定のkeyを削除する
		T lastKey = getAndRemoveLast(keyList);
		if (!lastKey.equals(key)) {
			keyList.set(kp.keyPostion, lastKey);
			keyPositionMap.get(lastKey).keyPostion = kp.keyPostion;
		}
		return true;
	}


	/**
	 * @param keys RandomKeySelectorに管理させるKeyのコレクション
	 * @param random RandomKeySelectorが使用する乱数生成器
	 *
	 * @throws IllegalArgumentException keysがnullを含む場合
	 */
	public RandomKeySelector(@Nonnull Collection<T> keys, @NonNull Random random) throws IllegalArgumentException {
		this.random = random;
		keyList = new ArrayList<T>(keys);
		for (T key : keys) {
			if (key == null) {
				throw new IllegalArgumentException(ERROR_NULL_ELEMENTS);
			}
		}
		initKeyPostionMap();
		initAloKeyList();
	}


	/**
	 * keyPositionMapをkeyListにマッチするように更新する
	 */
	private void initKeyPostionMap() {
		for(int i = 0; i < keyList.size(); i++) {
			KeyPosition kp = keyPositionMap.put(keyList.get(i), new KeyPosition(i, -1));
			if (kp != null) {
			throw new IllegalArgumentException(ERROR_DUPLICATE_ELEMENTS + keyList.get(i) + ".");
			}
		}
	}


	/**
	 * keyListからinitKeyListを生成し、keyPositionMapを更新する。
	 */
	private void initAloKeyList() {
		aloKeyList = new ArrayList<>(keyList);
		for (int i = aloKeyList.size() - 1; i > 0; i--) {
			int j = random.nextInt(i);
			T tmpKey = aloKeyList.get(i);
			aloKeyList.set(i, aloKeyList.get(j));
			aloKeyList.set(j, tmpKey);
		}
		updateKeyPositionMap();
	}

	/**
	 * keyPositionMapをaoeKeyListにマッチするように更新する
	 */
	private void updateKeyPositionMap() {
		for(int i = 0; i < aloKeyList.size(); i++) {
			keyPositionMap.get(aloKeyList.get(i)).aloKeyPostion = i;
		}
	}


	/**
	 * 指定のリストから最後の要素を取り出し、最後の要素を削除する。
	 *
	 * @param list
	 * @return
	 */
	private T getAndRemoveLast(List<T> list) {
		int size = list.size();
		if (size == 0) {
			return null;
		}
		T ret = list.get(size -1);
		list.remove(size -1);
		return ret;
	}


	/**
	 * 指定のKeyを追加する
	 */
	public synchronized void add(@NonNull T key) {
		keyList.add(key);
		keyPositionMap.put(key, new KeyPosition(keyList.size() -1 , -1));
	}


	/**
	 * リストのKeyの位置を表すクラス
	 */
	static class KeyPosition {
		public KeyPosition(int keyPostion, int aloKeyPostion) {
			this.keyPostion = keyPostion;
			this.aloKeyPostion = aloKeyPostion;
		}

		/**
		 *  aloKeyList中の位置、 aloKeyPostionにkeyが含まれない場合-1
		 */
		int aloKeyPostion;


		/**
		 *  keyList中の位置
		 */
		int keyPostion;


		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + aloKeyPostion;
			result = prime * result + keyPostion;
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
			KeyPosition other = (KeyPosition) obj;
			if (aloKeyPostion != other.aloKeyPostion)
				return false;
			if (keyPostion != other.keyPostion)
				return false;
			return true;
		}


		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("KeyPosition [keyPostion=");
			builder.append(keyPostion);
			builder.append(", aloKeyPostion=");
			builder.append(aloKeyPostion);
			builder.append("]");
			return builder.toString();
		}
	}
}
