package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeUtils;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.sql.result.mapping.TgEntityResultMapping;

public class ContractDaoIceaxe implements ContractDao {
    // TODO tsurugiがis not nullを使えないため、nullの代わりにLocalDate.MAXを使用している => is not nullサポート後にnullを使用するように変更する

    private final IceaxeUtils utils;

    public ContractDaoIceaxe(PhoneBillDbManagerIceaxe manager) {
        utils = new IceaxeUtils(manager);
    }

    private static final TgEntityResultMapping<Contract> RESULT_MAPPING =
            TgResultMapping.of(Contract::new)
            .addString("phone_number", Contract::setPhoneNumber)
            .addDate("start_date", Contract::setStartDate)
            .addDate("end_date", Contract::setEndDate)
            .addString("charge_rule", Contract::setRule);

    private static final TgParameterMapping<Contract> PARAMETER_MAPPING = TgParameterMapping.of(Contract.class)
            .add("phone_number", TgDataType.STRING, Contract::getPhoneNumber)
            .add("start_date", TgDataType.DATE, Contract::getStartDateAsLocalDate)
            .add("end_date", TgDataType.DATE, Contract::getEndDateAsLocalDate)
            .add("charge_rule", TgDataType.STRING, Contract::getRule);

    private TsurugiSqlPreparedStatement<Contract> createInsertPs() {
        String sql = "insert into contracts(phone_number, start_date, end_date, charge_rule) "
                + "values(:phone_number, :start_date, :end_date, :charge_rule)";
        return utils.createPreparedStatement(sql, PARAMETER_MAPPING);
    }

    private TsurugiSqlPreparedStatement<Contract> createUpdatePs() {
        String sql = "update contracts set end_date = :end_date, charge_rule = :charge_rule"
                + " where phone_number = :phone_number and start_date = :start_date";
        return utils.createPreparedStatement(sql, PARAMETER_MAPPING);
    }


    @Override
    public int[] batchInsert(Collection<Contract> contracts) {
        return utils.executeAndGetCount(createInsertPs(), contracts);
    }


    @Override
    public int insert(Contract contract) {
        return utils.executeAndGetCount(createInsertPs(), contract);
    }

    @Override
    public int update(Contract contract) {
        return utils.executeAndGetCount(createUpdatePs(), contract);
    }

    @Override
    public List<Contract> getContracts(String phoneNumber) {
        String sql = "select start_date, end_date, charge_rule from contracts where phone_number = :phone_number order by start_date";
        var variables = TgBindVariables.of().addString("phone_number");
        var ps = utils.createPreparedQuery(sql, TgParameterMapping.of(variables));
        var parameter = TgBindParameters.of().add("phone_number", phoneNumber);
        var list = utils.execute(ps, parameter);
        return list.stream().map(r -> createContract(phoneNumber, r)).collect(Collectors.toList());
    }

    private Contract createContract(String phoneNumber, TsurugiResultEntity r) {
        Contract c = new Contract();
        c.setPhoneNumber(phoneNumber);
        c.setStartDate(r.getDate("start_date"));
        c.setEndDate(r.getDateOrNull("end_date"));
        c.setRule(r.getStringOrNull("charge_rule"));
        return c;
    }

    @Override
    public Contract getContract(Key key) {
        String sql = "select start_date, end_date, charge_rule from contracts where phone_number = :phone_number and start_date = :start_date";
        var variables = TgBindVariables.of().addString("phone_number").addDate("start_date");
        var ps = utils.createPreparedQuery(sql, TgParameterMapping.of(variables));
        var parameter = TgBindParameters.of().add("phone_number", key.getPhoneNumber()).add("start_date", key.getStartDateAsLocalDate());
        var list = utils.execute(ps, parameter);
        if (list.size() == 0) {
            return null;
        }
        Contract c = new Contract();
        c.setPhoneNumber(key.getPhoneNumber());
        c.setStartDate(key.getStartDateAsLong());
        c.setEndDate(list.get(0).getDateOrNull("end_date"));
        c.setRule(list.get(0).getStringOrNull("charge_rule"));
        return c;
    }

    @Override
    public List<Contract> getContracts(Date start, Date end) {
// TODO: is not nullが使用可能になった後に使用するSQL
//		String sql = "select phone_number, start_date, end_date, charge_rule"
//				+ " from contracts where start_date <= :start_date and ( end_date is null or end_date >= :end_date)"
//				+ " order by phone_number";
        String sql = "select phone_number, start_date, end_date, charge_rule"
                + " from contracts where start_date <= :start_date and end_date >= :end_date"
                + " order by phone_number";
        var variables = TgBindVariables.of().addDate("start_date").addDate("end_date");
        var ps = utils.createPreparedQuery(sql, TgParameterMapping.of(variables), RESULT_MAPPING);
        var parameter = TgBindParameters.of().add("end_date", start.toLocalDate() ).add("start_date", end.toLocalDate());
        return utils.execute(ps, parameter);
    }

    @Override
    public List<Contract> getContracts() {
        String sql = "select phone_number, start_date, end_date, charge_rule from contracts";
        var ps = utils.createPreparedQuery(sql, RESULT_MAPPING);
        return utils.execute(ps);
    }

    @Override
    public List<String> getAllPhoneNumbers() {
        String sql = "select phone_number from contracts order by phone_number";
        var ps = utils.createPreparedQuery(sql);
        var list = utils.execute(ps);
        return list.stream().map(r -> r.getString("phone_number")).collect(Collectors.toList());
    }
    @Override
    public long count() {
        var ps = utils.createPreparedQuery("select count(*) as cnt from contracts");
        List<TsurugiResultEntity> list = utils.execute(ps);
        return list.get(0).findLong("cnt").orElse(0L);
    }

    @Override
    public int delete(Key key) {
        String sql = "delete from contracts where phone_number = :phone_number and start_date = :start_date";
        TgParameterMapping<Key> parameterMapping = TgParameterMapping.of(Key.class)
                .add("phone_number", TgDataType.STRING, Key::getPhoneNumber)
                .add("start_date", TgDataType.DATE, Key::getStartDateAsLocalDate);
        var ps = utils.createPreparedStatement(sql, parameterMapping);
        return utils.executeAndGetCount(ps, key);
    }


    @Override
    public List<Key> getAllPrimaryKeys() {
        String sql = "select phone_number, start_date from contracts";
        var ps = utils.createPreparedQuery(sql);
        List<TsurugiResultEntity> resultList = utils.execute(ps);
        ArrayList<Key> list = new ArrayList<>(resultList.size());
        for(TsurugiResultEntity entry: resultList) {
            Key key = new Key();
            key.setPhoneNumber(entry.getString("phone_number"));
            key.setStartDate(entry.getDate("start_date"));
            list.add(key);
        }
        return list;
    }
}
