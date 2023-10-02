package com.tsurugidb.benchmark.phonebill.testdata;

import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.jdbc.Duration;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

/**
 * 契約マスタの情報を取得するためのクラス
 * <p>
 * 契約マスタはブロック単位で保持し以下の情報を保持する
 * <ul>
 *   <li>有効なブロックのブロック番号</li>
 *   <li>各ブロック内の位置によって決まる情報</li>
 *   <ul>
 *     <li></li>
 *     <li>初期契約期間:MasterUpdateAppにより更新される前のTestDataGeneratorにより生成された期間</li>
 *     <li>契約のキー値</li>
 *     <li>MasterDeleteInsertAppが生成する履歴の開始時刻に当該契約が有効/無効</li>
 *   </ul>
 * </ul>
 * 本クラスはスレッドセーフではない。本クラスを使用する各スレッドが本クラスのインスタンスを
 * 保持することを想定している。
 */
public class ContractInfoReader {
    private static final Logger LOG = LoggerFactory.getLogger(ContractInfoReader.class);


    /**
     * アクティブなブロックのブロック番号の情報
     */
    private ActiveBlockNumberHolder blockInfos;

    /**
     * ブロックのサイズ
     */
    private int blockSize;

    /**
     * 初期契約期間のリスト
     */
    private List<Duration> durationList;

    /**
     * 「MasterDeleteInsertAppが生成する履歴の開始時刻」に当該契約が有効/無効を表すリスト
     */
    private List<Boolean> statusList;

    /**
     * getKeyNewContract()による新規契約のキー払い出し時に、払い出すキーのブロック上の位置
     */
    private int posInBlock;

    /**
     * 契約のブロックに関する情報にアクセスするためのアクセサ
     */
    private ContractBlockInfoAccessor contractBlockInfoAccessor;


    /**
     * 新規契約のキー払い出しに使用しているブロックのブロック番号
     */
    private int blockNumber;


    /**
     * 電話番号生成器
     */
    private PhoneNumberGenerator phoneNumberGenerator;


    /**
     * 使用する乱数生成器
     */
    private Random random;

    /**
     * getNewContract()の戻り値用のインスタンス。高速化のためにインスタンスを使い回す。
     */
    private  Contract retContract;


    /**
     * Configに従いContractInfoReaderを初期化する。
     *
     * @param config コンフィグ
     * @param accessor 契約ブロックの情報にアクセスするためのアクセサ
     * @param random 使用する乱数生成器
     * @return
     * @throws IOException
     */
    public static ContractInfoReader create(Config config, ContractBlockInfoAccessor accessor, Random random) throws IOException {
        // すべてのContractInfoReaderインスタンスが同じdurationListを持つように、
        // configで指定された乱数のシードの乱数生成器でdurationListを作成する
        List<Duration> durationList = initDurationList(config);
        long time = getHistoryInsertAppStartTimeDate(config).getTime();
        List<Boolean> statusList = new ArrayList<>(durationList.size());
        for (int i = 0; i < durationList.size(); i++) {
            Duration d = durationList.get(i);
            if (d.end == null) {
                statusList.add(d.start <= time);
            } else {
                statusList.add(d.start <= time && time < d.end);
            }
        }
        PhoneNumberGenerator phoneNumberGenerator = new PhoneNumberGenerator(config);
        ContractInfoReader reader = new ContractInfoReader(durationList, statusList, accessor, phoneNumberGenerator, random);
        int bs = reader.getBlockSize();

        // このassertでエラーが起きるのはinitDurationList()の修正に合わせて、getContractBlockSize()を修正していないことを示唆する
        assert getContractBlockSize(config) == bs;

        long cs = config.numberOfContractsRecords;
        if (cs < bs ) {
            String format = "numberOfContractsRecords(%d) must be larger than contract block size(%d).";
            throw new RuntimeException(String.format(format, cs, bs));
        }
        return reader;
    }



    /**
     * 個別にパラメータを指定可能なコンストラクタ。UT以外で使用禁止。
     *
     * @param durationList
     * @param contractKeyList
     * @param statusList
     * @param numberOfPhonumberInBlock
     * @param contractBlockInfoAccessor
     * @param phoneNumberGenerator
     * @param random
     * @throws IOException
     */
    ContractInfoReader(List<Duration> durationList, List<Boolean> statusList,
            ContractBlockInfoAccessor contractBlockInfoAccessor, PhoneNumberGenerator phoneNumberGenerator,
            Random random) throws IOException {
        this.durationList = durationList;
        this.statusList = statusList;
        this.contractBlockInfoAccessor = contractBlockInfoAccessor;
        this.phoneNumberGenerator = phoneNumberGenerator;
        this.random = random;
        if (durationList.size() != statusList.size()) {
            throw new IllegalArgumentException("Array size mismatch.");
        }
        blockSize = durationList.size();
        loadActiveBlockNumberList();
        posInBlock = 0;
        blockNumber = -1;
        retContract = new Contract();
        retContract.setRule("dummy");
    }

    /**
     * アクティブなブロック番号のリストをロードする。
     * @throws IOException
     */
    public final void loadActiveBlockNumberList() throws IOException {
        LOG.debug("Loading active block number list.");
        blockInfos = contractBlockInfoAccessor.getActiveBlockInfo();
    }



    /**
     * 新規に作成する契約を返す
     * <br>
     * このメソッドが呼ばれるたびに新規の契約を作成する。実際にその契約のデータが生成されるかについては関知しないので
     * 呼び出し側で確実に契約を作成する必要がある。
     *
     * @return
     * @throws IOException
     */
    public Contract getNewContract() throws IOException {
        if (blockNumber < 0) {
            newBlock();
        }
        long n = blockNumber * getBlockSize() + posInBlock++;
        retContract.setPhoneNumber(phoneNumberGenerator.getPhoneNumber(n));
        Duration d = getInitialDuration(n);
        retContract.setStartDate(d.getStatDate());
        retContract.setEndDate(d.getEndDate());
        if (posInBlock >= blockSize) {
            contractBlockInfoAccessor.submit(blockNumber);
            blockNumber = -1;
        }

        return retContract;
    }


    /**
     * 更新対象の契約を選択しそのKeyを返す。
     *
     * @return
     */
    public Key getKeyUpdatingContract() {
        Key retKey = new Key();
        // 次の２つの式は乱数生成器の使用順が固定されるように、意図的に2行に分けている
        long n = getRandomBlockNumber() * getBlockSize();
        n =  n + random.nextInt(getBlockSize());
        retKey.setPhoneNumber(phoneNumberGenerator.getPhoneNumber(n));
        retKey.setStartDate(durationList.get((int) (n % getBlockSize())).getStatDate());
        return retKey;
    }

    /**
     * n番目の契約が有効な契約かどうかを返す
     *
     * @param n
     * @return 有効な契約の場合true
     */
    public boolean isActive(long n) {
        int s = (int) (n % getBlockSize());
        return statusList.get(s);
    }


    /**
     * n番目の契約の初期契約期間を返す。
     *
     * @param n
     * @return
     */
    public Duration getInitialDuration(long n) {
        return durationList.get((int) (n % getBlockSize()));
    }


    /**
     * n番目の契約の電話番号をlong値で取得する
     *
     * @param n
     * @return
     */
    public long getPhoneNumberAsLong(long n) {
        return phoneNumberGenerator.getPhoneNumberAsLong(n);
    }


    /**
     * ランダムにブロックを選択し、選択したブロック上のposで指定された位置が
     * 何番目の契約かを返す。
     *
     * @param pos
     * @return
     */
    public long getRandomN(int pos) {
        return getRandomBlockNumber() + pos;
    }

    /**
     * 新しいブロックを確保する
     * @throws IOException
     */
    private void newBlock() throws IOException {
        blockNumber = contractBlockInfoAccessor.getNewBlock();
        posInBlock = 0;
    }

    /**
     * 有効なブロックをランダムに選択し、選択したブロックのブロック番号を返す。
     *
     * @return
     */
    int getRandomBlockNumber() {
        int n = random.nextInt(blockInfos.getNumberOfActiveBlacks());
        int mbn = blockInfos.getMaximumBlockNumberOfFirstConsecutiveActiveBlock();
        if (mbn == -1) {
            return blockInfos.getActiveBlocks().get(n);
        }
        if (n <= mbn) {
            return n;
        }
        return blockInfos.getActiveBlocks()
                .get(n - blockInfos.getMaximumBlockNumberOfFirstConsecutiveActiveBlock() - 1);
    }


    /**
     * 契約日のパターンのリストを作成する
     * @return
     */
    static List<Duration> initDurationList(Config config) {
        Random random = new Random(config.randomSeed);
        List<Duration> list = new ArrayList<Duration>();

        // 契約終了日がないduration
        for (int i = 0; i < config.noExpirationDateRate; i++) {
            Date start = getDate(config.minDate, config.maxDate, random);
            list.add(new Duration(start, null));
        }
        // 契約終了日があるduration
        for (int i = 0; i < config.expirationDateRate; i++) {
            Date start = getDate(config.minDate, config.maxDate, random);
            Date end = getDate(start, config.maxDate, random);
            list.add(new Duration(start, end));
        }
        // 同一電話番号の契約が複数あるパターン用のduration
        for (int i = 0; i < config.duplicatePhoneNumberRate; i++) {
            Date end = getDate(config.minDate, DateUtils.previousMonthLastDay(config.maxDate), random);
            Date start = getDate(DateUtils.nextMonth(end), config.maxDate, random);
            list.add(new Duration(config.minDate, end));
            list.add(new Duration(start, null));
        }
        return list;

    }

    /**
     * min～maxの範囲のランダムな日付を取得する
     *
     * @param min
     * @param max
     * @return
     */
    static Date getDate(Date min, Date max, Random random) {
        int days = (int) ((max.getTime() - min.getTime()) / DateUtils.A_DAY_IN_MILLISECONDS);
        long offset = random.nextInt(days + 1) * DateUtils.A_DAY_IN_MILLISECONDS;
        return new Date(min.getTime() + offset);
    }

    /**
     * getHistoryInsertAppが生成する履歴データの通話開始時刻が属する日を返す
     * <p>
     * config.maxDateの翌日を返す
     *
     * @return
     */
    private static Date getHistoryInsertAppStartTimeDate(Config config) {
        return DateUtils.nextDate(config.maxDate);
    }


    public int getBlockSize() {
        return blockSize;
    }


    public ActiveBlockNumberHolder getBlockInfos() {
        return blockInfos;
    }

    /**
     * 指定のConfig値のときの契約マスタのブロックサイズを取得する
     *
     * @return
     */
    public static int getContractBlockSize(Config config) {
        return config.duplicatePhoneNumberRate * 2 + config.expirationDateRate + config.noExpirationDateRate;
    }

}
