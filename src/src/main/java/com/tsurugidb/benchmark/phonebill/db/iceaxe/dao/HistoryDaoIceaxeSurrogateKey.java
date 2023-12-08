package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTarget;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeUtils;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxeSurrogateKey;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;
import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgSingleParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.sql.result.mapping.TgEntityResultMapping;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;

/**
 * ソロゲートキーを使用時用のHistoryDao、サロゲートキーの値はStaticなAtomicLong型の
 * カウンタで生成している。このため、複数プロセス構成では正しく動作しない。
 */
public class HistoryDaoIceaxeSurrogateKey implements HistoryDao {
    private final IceaxeUtils utils;
    private final PhoneBillDbManagerIceaxeSurrogateKey manager;
    private static volatile AtomicLong sidCounter = null;

    private static final TgEntityResultMapping<History> RESULT_MAPPING =
            TgResultMapping.of(History::new)
            .addLong("sid", History::setSid)
            .addString("caller_phone_number", History::setCallerPhoneNumber)
            .addString("recipient_phone_number", History::setRecipientPhoneNumber)
            .addString("payment_category", History::setPaymentCategorty)
            .addDateTime("start_time", History::setStartTime)
            .addInt("time_secs", History::setTimeSecs)
            .addInt("charge", History::setCharge)
            .addInt("df", History::setDf);

    private static final TgParameterMapping<History> PARAMETER_MAPPING = TgParameterMapping.of(History.class)
            .add("sid", TgDataType.LONG, History::getSid)
            .add("caller_phone_number", TgDataType.STRING, History::getCallerPhoneNumber)
            .add("recipient_phone_number", TgDataType.STRING, History::getRecipientPhoneNumber)
            .add("payment_category", TgDataType.STRING, History::getPaymentCategorty)
            .add("start_time", TgDataType.DATE_TIME, History::getStartTimeAsLocalDateTime)
            .add("time_secs", TgDataType.INT, History::getTimeSecs)
            .add("charge", TgDataType.INT, History::getCharge).add("df", TgDataType.INT, History::getDf);


    public HistoryDaoIceaxeSurrogateKey(PhoneBillDbManagerIceaxeSurrogateKey manager) {
        utils = new IceaxeUtils(manager);
        initSidCounter(utils, manager);
        this.manager = manager;
    }


    /**
     *　sidCountを現在のテーブルデータの最大値に設定する
     *
     * @param utils
     * @param manager
     * @return
     */
    static synchronized void initSidCounter(IceaxeUtils utils, PhoneBillDbManagerIceaxe manager) {
        if (sidCounter == null) {
            sidCounter = new AtomicLong();
            manager.execute(TxOption.of(), () -> {
                var ps = utils.createPreparedQuery("select max(sid) as max_sid from history");
                List<TsurugiResultEntity> list = utils.execute(ps);
                long maxSid = list.get(0).findLong("max_sid").orElse(0L);
                sidCounter.set(maxSid);
            });
        }
    }

    @Override
    public int[] batchInsert(Collection<History> histories) {
        int[] ret = new int[histories.size()];
        var ps = createInsertPs();
        try (ps) {
            int i = 0;
            for (History h : histories) {
                h.setSid(sidCounter.incrementAndGet());
                ret[i++] = manager.getCurrentTransaction().executeAndGetCount(ps, h);
            }
            return ret;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    @Override
    public int insert(History h) {
        h.setSid(sidCounter.incrementAndGet());
        var ps = createInsertPs();
        return utils.executeAndGetCount(ps, h);
    }

    private TsurugiSqlPreparedStatement<History> createInsertPs() {
        String sql = "insert into history(sid, caller_phone_number, recipient_phone_number, payment_category, start_time, time_secs, charge, df) "
                + "values(:sid, :caller_phone_number, :recipient_phone_number, :payment_category, :start_time, :time_secs, :charge, :df)";
        return utils.createPreparedStatement(sql, PARAMETER_MAPPING);
    }

    private TsurugiSqlPreparedStatement<History> createUpdatePs() {
        String sql = "update history"
                + " set caller_phone_number = :caller_phone_number, recipient_phone_number = :recipient_phone_number, "
                + "payment_category = :payment_category, start_time = :start_time, time_secs = :time_secs, charge = :charge, df = :df"
                + " where  sid = :sid";
        return utils.createPreparedStatement(sql, PARAMETER_MAPPING);
    }

    @Override
    public long getMaxStartTime() {
        var ps = utils.createPreparedQuery("select max(start_time) as max_start_time from history");
        List<TsurugiResultEntity> list = utils.execute(ps);
        return DateUtils.toEpocMills(list.get(0).findDateTime("max_start_time").orElse(LocalDateTime.MIN));
    }

    @Override
    public int update(History history) {
        var ps = createUpdatePs();
        return utils.executeAndGetCount(ps, history);
    }

    @Override
    public int updateChargeNull() {
        var ps = utils.createPreparedStatement("update history set charge = null");
        return utils.executeAndGetCount(ps);
    }


    @Override
    public int batchUpdate(List<History> histories) {
        // TODO アップデートに成功した件数を返すようにする
        var ps = createUpdatePs();
        int[] rets = utils.executeAndGetCount(ps, histories);
        return rets.length;
    }

    /**
     * 指定の契約に紐付く通話履歴を取得する
     *
     * TODO: 現状のTsurugiではwhere句付きのJoinが正しく動作しないので、Joinを使用せずに2回に話変えてQueryを実行している。
     * Tsurugiが想定通りに動作するようになったら、オリジナルのコードに戻す。
     *
     */
    @Override
    public List<History> getHistories(Key key) {
        String sql1 = "select end_date from contracts where phone_number = :phone_number and start_date = :start_date";
        var variable1 = TgBindVariables.of().addString("phone_number").addDate("start_date");
        var ps1 = utils.createPreparedQuery(sql1, TgParameterMapping.of(variable1));
        var param1 = TgBindParameters.of()
                .add("phone_number", key.getPhoneNumber())
                .add("start_date", key.getStDateAsLocalDate());
        var list = utils.execute(ps1, param1);
        if (list.isEmpty()) {
            return Collections.emptyList();
        }

        LocalDate endDate = list.get(0).getDate("end_date");
        String sql2 = "select sid, caller_phone_number, recipient_phone_number, payment_category, start_time,"
                + " time_secs, charge, df from history where start_time >= :start_date and start_time < :end_date"
                + " and caller_phone_number = :phone_number";
        var variable2 = TgBindVariables.of().addString("phone_number").addDateTime("start_date").addDateTime("end_date");
        var ps = utils.createPreparedQuery(sql2, TgParameterMapping.of(variable2), RESULT_MAPPING);
        var param2 = TgBindParameters.of()
                .add("phone_number", key.getPhoneNumber())
                .add("start_date", key.getStartDateAsLocalDateTime())
                .add("end_date", LocalDateTime.of(
                        endDate.equals(LocalDate.MAX) ? LocalDate.MAX: endDate.plusDays(1), LocalTime.MIDNIGHT));
        return utils.execute(ps, param2);
    }

    /**
     * getHistories(Key key)のオリジナルコード、今は想定通りの動作をしない。
     *
     */
    public List<History> getHistoriesOrg(Key key) {
        String sql = "select"
                + " h.caller_phone_number, h.recipient_phone_number, h.payment_category, h.start_time, h.time_secs,"
                + " h.charge, h.df" + " from history h"
                + " inner join contracts c on c.phone_number = h.caller_phone_number"
                + " where c.start_date < h.start_time and" + " (h.start_time < c.end_date + "
                + DateUtils.A_DAY_IN_MILLISECONDS + " or c.end_date = " + Long.MAX_VALUE + ")"
                + " and c.phone_number = :phone_number and c.start_date = :start_date" + " order by h.start_time";

        var variables = TgBindVariables.of().addString("phone_number").addDate("start_date");
        var ps = utils.createPreparedQuery(sql, TgParameterMapping.of(variables), RESULT_MAPPING);
        var parameter = TgBindParameters.of()
                .add("phone_number", key.getPhoneNumber())
                .add("start_date", key.getStartDate().getTime());
        return utils.execute(ps, parameter);
    }



    @Override
    public List<History> getHistories(CalculationTarget target) {
        Contract contract = target.getContract();
        LocalDateTime start = LocalDateTime.of(target.getStart().toLocalDate(), LocalTime.MIDNIGHT);
        LocalDateTime end =  LocalDateTime.of(target.getEnd().toLocalDate(), LocalTime.MIDNIGHT) ;

//		TODO 現状のTsurugiはorを使った検索で意図したようにセカンダリインデックスを使用しないので二つのQueryに分けて
//		実行している。この問題が解決したら元のqueryに戻す。
//
//		String sql = "select caller_phone_number, recipient_phone_number, payment_category, start_time, time_secs,"
//				+ " charge, df" + " from history "
//				+ "where start_time >= :start and start_time < :end"
//				+ " and ((caller_phone_number = :caller_phone_number  and payment_category = 'C') "
//				+ "  or (recipient_phone_number = :recipient_phone_number and payment_category = 'R'))"
//				+ " and df = 0";



        String sql1 = "select sid, caller_phone_number, recipient_phone_number, payment_category, start_time, time_secs,"
                + " charge, df" + " from history "
                + "where start_time >= :start and start_time < :end"
                + " and recipient_phone_number = :recipient_phone_number and payment_category = 'R' and df = 0";
        String sql2 = "select sid, caller_phone_number, recipient_phone_number, payment_category, start_time, time_secs,"
                + " charge, df" + " from history "
                + "where start_time >= :start and start_time < :end"
                + " and caller_phone_number = :caller_phone_number  and payment_category = 'C' and df = 0";
        var variable1 = TgBindVariables.of().addDateTime("start").addDateTime("end").addString("recipient_phone_number")
                .addString("recipient_phone_number");
        var variable2 = TgBindVariables.of().addDateTime("start").addDateTime("end").addString("caller_phone_number")
                .addString("caller_phone_number");
        var ps1 = utils.createPreparedQuery(sql1, TgParameterMapping.of(variable1), RESULT_MAPPING);
        var ps2 = utils.createPreparedQuery(sql2, TgParameterMapping.of(variable2), RESULT_MAPPING);
        var param1 = TgBindParameters.of()
                .add("start", start)
                .add("end", end.plusDays(1))
                .add("recipient_phone_number",contract.getPhoneNumber());
        var param2 = TgBindParameters.of()
                .add("start", start)
                .add("end", end.plusDays(1))
                .add("caller_phone_number", contract.getPhoneNumber());
        List<History> list = utils.execute(ps1, param1);
        list.addAll(utils.execute(ps2, param2));
        return list;
    }

    @Override
    public List<History> getHistories() {
        String sql = "select sid, caller_phone_number, recipient_phone_number, payment_category, start_time, time_secs, charge, df from history";
        var ps = utils.createPreparedQuery(sql, RESULT_MAPPING);
        return utils.execute(ps);
    }


    @Override
    public int delete(String phoneNumber) {
        String sql = "delete from history where  caller_phone_number = :caller_phone_number";
        TgParameterMapping<String> parameterMapping = TgSingleParameterMapping.ofString("caller_phone_number");
        var ps = utils.createPreparedStatement(sql, parameterMapping);
        return utils.executeAndGetCount(ps, phoneNumber);
    }


    @Override
    public List<String> getAllPhoneNumbers() {
        String sql = "select caller_phone_number from history";
        var ps = utils.createPreparedQuery(sql);
        var list = utils.execute(ps);
        return list.stream().map(r -> r.getString("caller_phone_number")).collect(Collectors.toList());
    }

    @Override
    public long count() {
        var ps = utils.createPreparedQuery("select count(*) as cnt from history");
        List<TsurugiResultEntity> list = utils.execute(ps);
        return list.get(0).findLong("cnt").orElse(0L);
    }


    @Override
    public int delete() {
        String sql = "delete from history";
        var ps = utils.createPreparedStatement(sql);
        return utils.executeAndGetCount(ps);
    }

    /**
     * sidCounterにnullをセット(UT専用)
     */
    static void setSidCounterNull() {
        sidCounter = null;
    }

    /**
     * sidCounterに指定の値をセット(UT専用)
     */
    static void setSidCounter(long val) {
        sidCounter = new AtomicLong(val);
    }

    /**
     * sidCounterの値を取得する(UT専用)
     *
     * @return
     */
    static long getSidCounter() {
        return sidCounter.get();
    }

}
