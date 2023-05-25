package com.tsurugidb.benchmark.phonebill.online;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * バッチ and/or オンラインアプリで、特定のテーブルから1レコードをランダムに 選択している処理に対して以下の機能を提供する.
 * <p>
 * <ul>
 * <li>カバー率の指定:
 * テーブル全体の中から指定した割合のデータを選択対象にします。テーブルの全レコードのうち、カバー率の割合のレコードのみをRandomKeySelectorで管理することにより実現します。</li>
 * <li>at least once:
 * 一定回数以上のランダムな選択を行った場合に、選択対象のレコードが少なくとも1回は選択されることを保証します。</li>
 * <li>分布関数の指定: 一様分布以外にも任意の分布関数を指定して、レコードの選択確率を調整します。現在未実装。</li>
 * </ul>
 * Tには対象テーブルのPKまたはUniqKeyを表すEntityClassを指定してください。当該EntityClassは、適切なequals()とhashCode()を実装している必要があります。
 *
 */
public class RandomKeySelector<T> {
    private static final Logger LOG = LoggerFactory.getLogger(RandomKeySelector.class);


    static final String ERROR_NULL_ELEMENTS = "Invalid argument: he 'keys' collection cannot have null elements.";
    static final String ERROR_DUPLICATE_ELEMENTS =  "Invalid argument: The 'keys' collection have duplicate element: ";
    static final String ERROR_RANGE = "Invalid argument: aloSelectRate should be a value between 0 and 1 (inclusive).\"";

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
    Map<T, KeyPositions> keyPositionMap = new HashMap<>();

    /**
     * RandomKeySelectorが使用する乱数生成器
     */
    private Random random;


    /**
     *  get(), getAndRemove()を用いてキーを選択する際にatLeastOnceのリストからキーを選択する割合.
     */
    private double aloSelectRate;



    /**
     * 指定のKeyを削除する.
     * <p>
     * getAndRemoveを使用せずにdeleteを実行する場合に、deleteしたレコードのキーを
     * RandomKeySelectorの管理対象外にするために使用する
     *
     * @param key
     * @return 指定されたKeyを保持していた場合はtrue
     */
    public synchronized boolean remove(@Nonnull T key) {
        KeyPositions kp = keyPositionMap.get(key);
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
     * @param aloSelectRate get(), getAndRemove()を用いてキーを選択する際にatLeastOnceのリストからキーを選択する割合。0d〜1dの範囲で指定する。
     *
     * @throws IllegalArgumentException keysがnullを含む場合, aloSelectRateの範囲外の値が指定されたとき
     */
    public RandomKeySelector(@Nonnull Collection<T> keys, @Nonnull Random random, double aloSelectRate) throws IllegalArgumentException {
        this(keys, random, aloSelectRate, 1d);
    }

    /**
     * @param keys RandomKeySelectorに管理させるKeyのコレクション
     * @param random RandomKeySelectorが使用する乱数生成器
     * @param aloSelectRate get(), getAndRemove()を用いてキーを選択する際にatLeastOnceのリストからキーを選択する割合。0d〜1dの範囲で指定する。
     * @param coverRate カーバ率。RandomKeySelectorはkeysのうちcoverRateで指定した割合の要素を管理する。0d〜1dの範囲で指定する。
     *
     * @throws IllegalArgumentException keysがnullを含む場合, aloSelectRate, coverRateに0d〜1dの範囲外の値が指定されたとき
     */
    public RandomKeySelector(@Nonnull Collection<T> keys, @Nonnull Random random, double aloSelectRate, double coverRate) throws IllegalArgumentException {
        if (aloSelectRate < 0d || 1d < aloSelectRate ) {
            throw new IllegalArgumentException(ERROR_RANGE);
        }
        if (coverRate < 0d || 1d < coverRate ) {
            throw new IllegalArgumentException(ERROR_RANGE);
        }
        this.aloSelectRate = aloSelectRate;
        this.random = random;
        int myKeyListSize = (int) (keys.size() * coverRate);

        keyList = new ArrayList<T>(myKeyListSize);
        int c = 0;
        for (T key : keys) {
            if (++c > myKeyListSize) {
                break;
            }
            if (key == null) {
                throw new IllegalArgumentException(ERROR_NULL_ELEMENTS);
            }
            keyList.add(key);
        }
        initKeyPostionMap();
        initAloKeyList();
        LOG.info("Inialized with {} keys.", keys.size());
    }

    /**
     * keyPositionMapをkeyListにマッチするように更新する
     */
    private void initKeyPostionMap() {
        for(int i = 0; i < keyList.size(); i++) {
            KeyPositions kp = keyPositionMap.put(keyList.get(i), new KeyPositions(i, -1));
            if (kp != null) {
            throw new IllegalArgumentException(ERROR_DUPLICATE_ELEMENTS + keyList.get(i) + ".");
            }
        }
    }


    /**
     * keyListからaloKeyListを生成し、keyPositionMapを更新する。
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
     * 指定のリストから最後の要素を取り出し、最後の要素を削除する
     *
     * @param list
     * @return リストの最後の要素
     */
    private T getAndRemoveLast(List<T> list) {
        int size = list.size();
        T ret = list.get(size -1);
        list.remove(size -1);
        return ret;
    }


    /**
     * 指定のKeyを追加する.
     * <p>
     * inserrtしたレコードのキーを追加するために使用する
     */
    public synchronized void add(@Nonnull T key) {
        keyList.add(key);
        keyPositionMap.put(key, new KeyPositions(keyList.size() -1 , -1));
    }


    /**
     * ランダムに選択したキーを返す.
     * <p>
     * select/updateの対象選択時に使用する
     *
     * @return 選択したキー、RandomKeySelectorが管理しているキーのリストが空の場合null
     */
    public synchronized T get() {
        T key;
        if (random.nextDouble() < aloSelectRate) {
            key = getAndRemoveLast(aloKeyList);
            if (aloKeyList.isEmpty()) {
                initAloKeyList();
            }
            keyPositionMap.get(key).aloKeyPostion = -1;
        } else {
            int idx = random.nextInt(keyList.size());
            key = keyList.get(idx);
        }
        return key;
    }

    /**
     * ランダムに選択したキーを返し、当該キーを管理対象から外す.
     * <p>
     * deleteの対象選択時に使用する
     *
     * @return 選択したキー、RandomKeySelectorが管理しているキーのリストが空の場合null
     */
    public synchronized T getAndRemove() {
        T key = get();
        remove(key);
        return key;
    }

    /**
     * リストのKeyの位置を表すクラス
     */
    static class KeyPositions {
        public KeyPositions(int keyPostion, int aloKeyPostion) {
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
    }
}
