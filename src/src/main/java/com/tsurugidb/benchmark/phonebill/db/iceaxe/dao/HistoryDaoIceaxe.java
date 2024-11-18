/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTarget;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeUtils;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe.InsertType;
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

public class HistoryDaoIceaxe implements HistoryDao {
    private final IceaxeUtils utils;
    private final InsertType insertType;

    private static final TgEntityResultMapping<History> RESULT_MAPPING =
            TgResultMapping.of(History::new)
            .addString("caller_phone_number", History::setCallerPhoneNumber)
            .addString("recipient_phone_number", History::setRecipientPhoneNumber)
            .addString("payment_category", History::setPaymentCategorty)
            .addDateTime("start_time", History::setStartTime)
            .addInt("time_secs", History::setTimeSecs)
            .addInt("charge", History::setCharge)
            .addInt("df", History::setDf);

    private static final TgParameterMapping<History> PARAMETER_MAPPING = TgParameterMapping.of(History.class)
            .add("caller_phone_number", TgDataType.STRING, History::getCallerPhoneNumber)
            .add("recipient_phone_number", TgDataType.STRING, History::getRecipientPhoneNumber)
            .add("payment_category", TgDataType.STRING, History::getPaymentCategorty)
            .add("start_time", TgDataType.DATE_TIME, History::getStartTimeAsLocalDateTime)
            .add("time_secs", TgDataType.INT, History::getTimeSecs)
            .add("charge", TgDataType.INT, History::getCharge).add("df", TgDataType.INT, History::getDf);

            private static final TgParameterMapping<History> PARAMETER_MAPPING_UPDATE_NON_KEY_FIELDS = TgParameterMapping.of(History.class)
            .add("caller_phone_number", TgDataType.STRING, History::getCallerPhoneNumber)
            .add("payment_category", TgDataType.STRING, History::getPaymentCategorty)
            .add("start_time", TgDataType.DATE_TIME, History::getStartTimeAsLocalDateTime)
            .add("time_secs", TgDataType.INT, History::getTimeSecs)
            .add("charge", TgDataType.INT, History::getCharge).add("df", TgDataType.INT, History::getDf);



    public HistoryDaoIceaxe(PhoneBillDbManagerIceaxe manager) {
        utils = new IceaxeUtils(manager);
        insertType = manager.getInsertType();
    }


    @Override
    public int[] batchInsert(Collection<History> histories) {
        var ps = createInsertPs();
        return utils.executeAndGetCount(ps, histories);
    }

    @Override
    public int insert(History h) {
        var ps = createInsertPs();
        return utils.executeAndGetCount(ps, h);
    }

    private TsurugiSqlPreparedStatement<History> createInsertPs() {
        String sql = insertType.getSqlInsertMethod()
                + " into history(caller_phone_number, recipient_phone_number, payment_category, start_time, time_secs, charge, df) "
                + "values(:caller_phone_number, :recipient_phone_number, :payment_category, :start_time, :time_secs, :charge, :df)";
        return utils.createPreparedStatement(sql, PARAMETER_MAPPING);
    }

    private TsurugiSqlPreparedStatement<History> createUpdatePs() {
        String sql = "update history"
                + " set recipient_phone_number = :recipient_phone_number, time_secs = :time_secs, charge = :charge, df = :df"
                + " where caller_phone_number = :caller_phone_number and payment_category = :payment_category and start_time = :start_time";
        return utils.createPreparedStatement(sql, PARAMETER_MAPPING);
    }

    private TsurugiSqlPreparedStatement<History> createUpdateNonKeyFieldsPs() {
        String sql = "update history"
                + " set time_secs = :time_secs, charge = :charge, df = :df"
                + " where caller_phone_number = :caller_phone_number and payment_category = :payment_category and start_time = :start_time";
        return utils.createPreparedStatement(sql, PARAMETER_MAPPING_UPDATE_NON_KEY_FIELDS);
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
    public int updateNonKeyFields(History history) {
        var ps = createUpdateNonKeyFieldsPs();
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
        // TODO アップデートに失敗した(アップデートの戻り値が0)例外をスローする
        var ps = createUpdatePs();
        int[] rets = utils.executeAndGetCount(ps, histories);
        return rets.length;
    }

    @Override
    public int batchUpdateNonKeyFields(List<History> histories) {
        // TODO アップデートに成功した件数を返すようにする
        // TODO アップデートに失敗した(アップデートの戻り値が0)例外をスローする
        var ps = createUpdateNonKeyFieldsPs();
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

        LocalDate endDate = list.get(0).getDateOrNull("end_date");
        String sql2 = "select caller_phone_number, recipient_phone_number, payment_category, start_time,"
                + " time_secs, charge, df from history where start_time >= :start_date"
                + " and caller_phone_number = :phone_number";
        var variable2 = TgBindVariables.of().addString("phone_number").addDateTime("start_date");
        var param2 = TgBindParameters.of()
                .add("phone_number", key.getPhoneNumber())
                .add("start_date", key.getStartDateAsLocalDateTime());
        if (endDate != null) {
            sql2 += " and start_time < :end_date";
            variable2.addDateTime("end_date");
            param2.add("end_date", LocalDateTime.of(endDate.plusDays(1), LocalTime.MIDNIGHT));
        }
        var ps = utils.createPreparedQuery(sql2, TgParameterMapping.of(variable2), RESULT_MAPPING);
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

		String sql = "select caller_phone_number, recipient_phone_number, payment_category, start_time, time_secs,"
				+ " charge, df" + " from history "
				+ "where start_time >= :start and start_time < :end"
				+ " and ((caller_phone_number = :caller_phone_number  and payment_category = 'C') "
				+ "  or (recipient_phone_number = :recipient_phone_number and payment_category = 'R'))"
				+ " and df = 0";

        var variable = TgBindVariables.of().addDateTime("start").addDateTime("end").addString("recipient_phone_number")
                .addString("caller_phone_number");
        var ps = utils.createPreparedQuery(sql, TgParameterMapping.of(variable), RESULT_MAPPING);
        var param = TgBindParameters.of()
                .add("start", start)
                .add("end", end.plusDays(1))
                .add("recipient_phone_number",contract.getPhoneNumber())
                .add("caller_phone_number", contract.getPhoneNumber());
        List<History> list = utils.execute(ps, param);
        return list;
    }

    @Override
    public List<History> getHistories() {
        String sql = "select caller_phone_number, recipient_phone_number, payment_category, start_time, time_secs, charge, df from history";
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

}
