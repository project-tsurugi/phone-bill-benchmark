package com.tsurugidb.benchmark.phonebill.app.durability;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;

public class ConfirmDbIntegrity {
    private static final Logger LOG = LoggerFactory.getLogger(ConfirmDbIntegrity.class);


    private String beforePrefix;
    private String configPath;
    private String batchLogPath;
    private String afterPrefix;
    private String reportPath;
    private static boolean exactlyEqual = true;


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        if (args.length == 5) {
            ConfirmDbIntegrity confirmDbIntegrity = new ConfirmDbIntegrity();
            confirmDbIntegrity.configPath = args[0];
            confirmDbIntegrity.batchLogPath = args[1];
            confirmDbIntegrity.beforePrefix = args[2];
            confirmDbIntegrity.afterPrefix = args[3];
            confirmDbIntegrity.reportPath = args[4];

            confirmDbIntegrity.exec();
        } else {
            usage();
            System.exit(1);
        }

        if (exactlyEqual) {
            String successMessage = "Database consistency has been confirmed.";
            LOG.info(successMessage);
            System.out.println(successMessage);
        } else {
            String errorMessage = "Database inconsistency detected.";
            LOG.error(errorMessage);
            System.err.println(errorMessage);
            System.exit(1);
        }
    }

    private static void usage() {
        System.out.println("Usage: java ConfirmDbIntegrity <config-path> <batch-log-path> <before-prefix> <after-prefix> <report-path>");
    }

    private void exec()
            throws ClassNotFoundException, IOException {

        Config config = Config.getConfig(configPath);
        List<History> actualHistories = new ArrayList<>();
        List<Contract> actualContracts = new ArrayList<>();
        List<Billing> actualBillings = new ArrayList<>();

        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            manager.execute(TxOption.of(), () -> {
                actualHistories.addAll(manager.getHistoryDao().getHistories());
                actualContracts.addAll(manager.getContractDao().getContracts());
                actualBillings.addAll(manager.getBillingDao().getBillings());
            });
        }

        DatabaseSnapshot expected = createExpected();

        try (PrintWriter out = new PrintWriter(new OutputStreamWriter(
                new FileOutputStream(reportPath, false), StandardCharsets.UTF_8))) {
            compareAndReport(out, expected.getHistories(), actualHistories,
                    History::getKey, Object::equals, "History");
            compareAndReport(out, expected.getContracts(), actualContracts,
                    Contract::getKey, Object::equals, "Contract");
            compareAndReport(out, expected.getBillings(), actualBillings,
                    Billing::getPhoneNumber, new BillingEqualityIgnoringBatchExecId(), "Billing");
        }
    }


    /**
     * 予想されるDatabaseSnapshotを生成します。これはコミットされた電話番号のリストを基にして、
     * 'before' スナップショットと 'after' スナップショットから得られた情報を統合して作成されます。
     * 'before' スナップショットからはcontractsを、'phoneNumbers' に含まれる電話番号に関連する
     * billingsとhistoriesを 'after' スナップショットから取得します。
     * 'phoneNumbers' に含まれない電話番号に関しては、'before' スナップショットのhistoriesが使われます。
     * このプロセスにおいて、'before' と 'after' のhistoriesのキーセットが異なる場合、
     * RuntimeExceptionがスローされます。
     *
     * @return 統合された情報を持つ新しいDatabaseSnapshotオブジェクト
     * @throws RuntimeException 'before' と 'after' のhistoriesのキーセットが一致しない場合
     */
    private DatabaseSnapshot createExpected() {
        // コミットされた電話番号のリストを取得し、それをSetに変換して性能を向上
        Set<String> phoneNumbersSet = new HashSet<>(getCommittedPhoneNumbers());

        // 'before' と 'after' のスナップショットを読み込む
        DatabaseSnapshot before = readDatabaseSnapshotFromFile(beforePrefix);
        DatabaseSnapshot after = readDatabaseSnapshotFromFile(afterPrefix);

        // 電話番号をキーとするHistoryマップを作成
        Map<String, List<History>> beforeHistoriesMap = mapPhoneNumbersToHistories(before.getHistories());
        Map<String, List<History>> afterHistoriesMap = mapPhoneNumbersToHistories(after.getHistories());

        // キーセットが同じであることを確認
        if (!beforeHistoriesMap.keySet().equals(afterHistoriesMap.keySet())) {
            throw new RuntimeException("The key sets of before and after history maps do not match.");
        }

        // 'phoneNumbers' に含まれないキーセットの差分を作成
        Set<String> excludedKeys = new HashSet<>(beforeHistoriesMap.keySet());
        excludedKeys.removeAll(phoneNumbersSet);

        // 'phoneNumbers' に含まれるキーセットを取得
        Set<String> includedKeys = new HashSet<>(phoneNumbersSet);
        includedKeys.retainAll(afterHistoriesMap.keySet());

        // 最終的なHistoryリストを構築
        List<History> finalHistories = new ArrayList<>();
        includedKeys.forEach(key -> finalHistories.addAll(afterHistoriesMap.get(key)));
        excludedKeys.forEach(key -> finalHistories.addAll(beforeHistoriesMap.get(key)));

        // 'phoneNumbersSet' に含まれる電話番号のみを持つbillingsを 'after' スナップショットからフィルタリング
        List<Billing> filteredBillings = after.getBillings().stream()
            .filter(billing -> phoneNumbersSet.contains(billing.getPhoneNumber()))
            .collect(Collectors.toList());

        // 'before' のcontractsと 'phoneNumbersSet' に含まれる電話番号を持つbillingsを保持したDatabaseSnapshotを構築
        DatabaseSnapshot finalSnapshot = new DatabaseSnapshot(finalHistories, before.getContracts(), filteredBillings);

        // 更新されたDatabaseSnapshotを返す
        return finalSnapshot;
    }


    /**
     * Historyリストから電話番号をキーにしたHistoryオブジェクトのリストのマップを作成します。
     * 'C'のpaymentCategoryではcallerPhoneNumberを、'R'ではrecipientPhoneNumberをキーとします。
     * paymentCategoryが'C'または'R'以外の値の場合はRuntimeExceptionをスローします。
     *
     * @param histories Historyオブジェクトのリスト
     * @return 電話番号をキー、Historyオブジェクトのリストを値とするマップ
     * @throws RuntimeException paymentCategoryが'C'または'R'以外の場合
     */
    public static Map<String, List<History>> mapPhoneNumbersToHistories(List<History> histories) {
        Map<String, List<History>> phoneNumberHistoryMap = new HashMap<>();

        for (History history : histories) {
            String keyPhoneNumber;

            switch (history.getPaymentCategorty()) {
                case "C":
                    keyPhoneNumber = history.getCallerPhoneNumber();
                    break;
                case "R":
                    keyPhoneNumber = history.getRecipientPhoneNumber();
                    break;
                default:
                    // paymentCategoryが'C'または'R'以外の場合、RuntimeExceptionをスロー
                    throw new RuntimeException("Invalid paymentCategory: " + history.getPaymentCategorty());
            }

            // 電話番号のキーがない場合はリストを作成しHistoryを追加
            phoneNumberHistoryMap.computeIfAbsent(keyPhoneNumber, k -> new ArrayList<>()).add(history);
        }

        return phoneNumberHistoryMap;
    }

    /**
     * バッチのログからコミット済みの電話番号のセットを作成する。
     * 同じ電話番号がログに2回以上現れた場合はRuntimeExceptionをスローする。
     *
     * @return 電話番号のセット
     * @throws RuntimeException 同じ電話番号がログに2回以上現れた場合
     */
    Set<String> getCommittedPhoneNumbers() {
        Set<String> phoneNumbers = new HashSet<>();
        // 「Transaction completed,」が含まれ、その後に「key =」と数字が続くパターン
        Pattern pattern = Pattern.compile("Transaction completed,.*key = (\\d+),");

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(batchLogPath), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    String phoneNumber = matcher.group(1);
                    // すでにセットに電話番号が存在する場合は例外をスロー
                    if (!phoneNumbers.add(phoneNumber)) {
                        throw new RuntimeException("Duplicate phone number found: " + phoneNumber);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return phoneNumbers;
    }




    private DatabaseSnapshot readDatabaseSnapshotFromFile(String prefix) {
        List<History> afterHistories = readListFromFile(prefix, History.class);
        List<Billing> afterBillings = readListFromFile(prefix, Billing.class);
        List<Contract> afterContracts = readListFromFile(prefix, Contract.class);
        return new DatabaseSnapshot(afterHistories, afterContracts, afterBillings);
    }

    private <T, K> void compareAndReport(
            PrintWriter out,
            List<T> expectedList,
            List<T> actualList,
            Function<T, K> keyExtractor,
            BiPredicate<T, T> customEquality,
            String typeName
    ) {
        EntityComparator<T, K> comparator = new EntityComparator<>(expectedList, actualList, keyExtractor,
                customEquality, typeName);
        if (comparator.areListsExactlyEqual()) {
            return;
        }
        exactlyEqual = false;
        out.println(typeName + " comparison Result:");
        out.println(comparator.getComparisonResultAsString());
    }


    @SuppressWarnings("unchecked")
    private <T extends Serializable> List<T> readListFromFile(String prefix, Class<?> clazz)
             {
        Path path = FileNameUtil.buildFilePathWithClass(prefix, clazz);
        try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            return (List<T>) in.readObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
