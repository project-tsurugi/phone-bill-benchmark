package com.tsurugidb.benchmark.phonebill.app.durability;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    boolean exactlyEqual = true;
    PrintWriter out;


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        ConfirmDbIntegrity confirmDbIntegrity = new ConfirmDbIntegrity();
        if (args.length == 5) {
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

        if (confirmDbIntegrity.exactlyEqual) {
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

        // 現在のDBデータを読み込む

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

        // 'before' と 'after' のスナップショットを読み込む
        DatabaseSnapshot before = readDatabaseSnapshotFromFile(beforePrefix);
        DatabaseSnapshot after = readDatabaseSnapshotFromFile(afterPrefix);

        // 電話番号をキーとするHistoryマップを作成
        Map<String, Set<History>> beforeHistoriesMap = before.getPhoneNumberToHistories();
        Map<String, Set<History>> afterHistoriesMap = after.getPhoneNumberToHistories();
        Map<String, Set<History>> actualHistoriesMap = DatabaseSnapshot.mapPhoneNumbersToHistories(actualHistories);

        // 電話番号をキーとする、Billingマップを作成
        Map<String, Billing> beforeBillingMap = before.getPhoneNumberToBilling();
        Map<String, Billing> afterBillingMap = after.getPhoneNumberToBilling();
        Map<String, Billing> actualBillingMap =
                DatabaseSnapshot.createMapFromList(actualBillings, Billing::getPhoneNumber);

        // キーセットが同じであることを確認
        if (!beforeHistoriesMap.keySet().equals(afterHistoriesMap.keySet())) {
            throw new RuntimeException("The key sets of before and after history maps do not match.");
        }
        if (!beforeHistoriesMap.keySet().equals(actualHistoriesMap.keySet())) {
            throw new RuntimeException("The key sets of before and actual history maps do not match.");
        }

        // ログファイルを解析する。
        Map<LogType, Map<String, String>> transactionIdKeyMaps;
        try (FileInputStream fis = new FileInputStream(batchLogPath)) {
            transactionIdKeyMaps = getAllTransactionIdKeyMaps(fis);
        }
        Map<String, String> abortedTransactions = transactionIdKeyMaps.get(LogType.ABORTED);
        Map<String, String> committingTransactions = transactionIdKeyMaps.get(LogType.COMMITTING);
        Map<String, String> completedTransactions = transactionIdKeyMaps.get(LogType.COMPLETED);

        // abortedTransactionsの電話番号がbeforeHistoriesMapのキーに含まれるか確認
        for (String phoneNumber : abortedTransactions.values()) {
            if (!beforeHistoriesMap.containsKey(phoneNumber)) {
                throw new RuntimeException("Invalid data found: Aborted transaction for phone number " + phoneNumber + " does not exist in before histories.");
            }
        }

        // committingTransactionsの電話番号がbeforeHistoriesMapのキーに含まれるか確認
        for (String phoneNumber : committingTransactions.values()) {
            if (!beforeHistoriesMap.containsKey(phoneNumber)) {
                throw new RuntimeException("Invalid data found: Committing transaction for phone number " + phoneNumber + " does not exist in before histories.");
            }
        }

        // completedTransactionsの電話番号がbeforeHistoriesMapのキーに含まれるか確認
        for (String phoneNumber : completedTransactions.values()) {
            if (!beforeHistoriesMap.containsKey(phoneNumber)) {
                throw new RuntimeException("Invalid data found: Completed transaction for phone number " + phoneNumber + " does not exist in before histories.");
            }
        }

        // 電話番号ごとにTXの状態を分類する
        Map<String, TransactionStatus> transactionStatuses = determineTransactionStatus(afterHistoriesMap.keySet(), abortedTransactions,
                committingTransactions, completedTransactions);

        try {out = new PrintWriter(new OutputStreamWriter(
                new FileOutputStream(reportPath, false), StandardCharsets.UTF_8));
            // Contractテーブルはバッチにより更新されないので変更されないことを確認する
            compareAndReport("Contract", Contract::getKey,
                    Object::equals, actualContracts, before.getContracts());
            // HistoryとBillingについては、電話番号ごとにTXの状態により処理を分ける。

            BillingEqualityIgnoringBatchExecId billingEquality = new BillingEqualityIgnoringBatchExecId();
            for(Entry<String, TransactionStatus>  entry: transactionStatuses.entrySet()) {
                String key = entry.getKey();
                String historyTitile = "History(phone number = " + entry.getKey() + ")";
                String billingTitile = "Billing(phone number = " + entry.getKey() + ")";
                Set<Billing> actualBillingSet = toBillingCollection(actualBillingMap.get(key));
                Set<Billing> beforeBillingSet = toBillingCollection(beforeBillingMap.get(key));
                Set<Billing> atterBillingSet = toBillingCollection(afterBillingMap.get(key));

                List<Collection<History>> expectedHistoriesList = new ArrayList<>(2);
                List<Collection<Billing>> expectedBillingList = new ArrayList<>(2);
                switch (entry.getValue()) {
                case COMMITTED:
                    expectedHistoriesList.add(afterHistoriesMap.get(key));
                    expectedBillingList.add(atterBillingSet);
                    break;
                case COMMITTING:
                    expectedHistoriesList.add(afterHistoriesMap.get(key));
                    expectedHistoriesList.add(beforeHistoriesMap.get(key));
                    expectedBillingList.add(atterBillingSet);
                    expectedBillingList.add(beforeBillingSet);
                    break;
                case UNCOMMITTED:
                    expectedHistoriesList.add(beforeHistoriesMap.get(key));
                    expectedBillingList.add(beforeBillingSet);
                    break;
                }
                compareAndReport(historyTitile,
                        History::getKey,
                        Object::equals,
                        actualHistoriesMap.get(key),
                        expectedHistoriesList);
                compareAndReport(billingTitile,
                        Billing::getPhoneNumber,
                        billingEquality,
                        actualBillingSet,
                        expectedBillingList);
            }
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }

    private Set<Billing> toBillingCollection(Billing billing) {
        if (billing == null) {
            return Collections.emptySet();
        }
        return Collections.singleton(billing);
    }


    /**
     * Compares an actual collection with a single expected collection and reports the comparison results.
     * If the actual collection matches the expected collection, the method returns without any action.
     *
     * @param <T> The type of elements in the collections.
     * @param <K> The type of the key used for comparison.
     * @param typeName The name of the type of elements being compared, used in the report output.
     * @param keyExtractor A function to extract comparison keys from the elements.
     * @param customEquality A BiPredicate that defines the equality logic for comparison.
     * @param actualCollection The collection of actual elements to compare.
     * @param expectedCollection The single expected collection to compare against the actual collection.
     */
    <T, K> void compareAndReport(
            String typeName,
            Function<T, K> keyExtractor,
            BiPredicate<T, T> customEquality,
            Collection<T> actualCollection,
            Collection<T> expectedCollection
    ) {
        compareAndReport(typeName, keyExtractor, customEquality, actualCollection,
                Collections.singletonList(expectedCollection));
    }


    /**
     * Compares an actual collection with multiple expected collections and reports the comparison results.
     * Iterates through each expected collection and compares it with the actual collection.
     * If the actual collection matches any of the expected collections, the method returns without further action.
     * If there is no match with any of the expected collections, the method reports the comparison results for each.
     *
     * @param <T> The type of elements in the collections.
     * @param <K> The type of the key used for comparison.
     * @param typeName The name of the type of elements being compared, used in the report output.
     * @param keyExtractor A function to extract comparison keys from the elements.
     * @param customEquality A BiPredicate that defines the equality logic for comparison.
     * @param actualCollection The collection of actual elements to compare.
     * @param expectedCollections A list of expected collections to compare against the actual collection.
     */
    <T, K> void compareAndReport(
            String typeName,
            Function<T, K> keyExtractor,
            BiPredicate<T, T> customEquality,
            Collection<T> actualCollection,
            List<Collection<T>> expectedCollections
    ) {
        boolean isMatchFound = false;

        for (Collection<T> expectedCollection : expectedCollections) {
            EntityComparator<T, K> comparator = new EntityComparator<>(expectedCollection, actualCollection,
                    keyExtractor, customEquality, typeName);
            if (comparator.areCollectionsExactlyEqual()) {
                isMatchFound = true;
                break;
            }
        }

        if (isMatchFound) {
            return;
        }

        exactlyEqual = false;

        for (int i = 0; i < expectedCollections.size(); i++) {
            Collection<T> expectedCollection = expectedCollections.get(i);
            EntityComparator<T, K> comparator = new EntityComparator<>(expectedCollection, actualCollection,
                    keyExtractor, customEquality, typeName);
            out.println(typeName + " comparison with expected collection #" + (i + 1) + " Result:");
            out.println(comparator.getComparisonResultAsString());
        }
    }






    // 電話番号とその出現回数を計算するヘルパーメソッド
    Map<String, Integer> calculatePhoneNumberOccurrences(Map<String, String> transactionMap) {
        Map<String, Integer> phoneNumberOccurrences = new HashMap<>();
        for (Map.Entry<String, String> entry : transactionMap.entrySet()) {
            String phoneNumber = entry.getValue();
            phoneNumberOccurrences.put(phoneNumber, phoneNumberOccurrences.getOrDefault(phoneNumber, 0) + 1);
        }
        return phoneNumberOccurrences;
    }


    /**
     * 各電話番号に対して、トランザクションの状態を決定する。
     *
     * @param phoneNumbers トランザクション状態を決定する電話番号のセット。
     * @param abortedTransactions 中止されたトランザクションの電話番号のマップ。
     * @param committingTransactions コミット中のトランザクションの電話番号のマップ。
     * @param completedTransactions コミット済みのトランザクションの電話番号のマップ。
     * @return 各電話番号に対するトランザクション状態のマップ。
     * @throws RuntimeException トランザクション状態の決定が不可能な場合。
     */
    public Map<String, TransactionStatus> determineTransactionStatus(
            Set<String> phoneNumbers,
            Map<String, String> abortedTransactions,
            Map<String, String> committingTransactions,
            Map<String, String> completedTransactions) {

        Map<String, TransactionStatus> phoneTransactionStatusMap = new HashMap<>();
        Map<String, Integer> abortedOccurrences = calculatePhoneNumberOccurrences(abortedTransactions);
        Map<String, Integer> committingOccurrences = calculatePhoneNumberOccurrences(committingTransactions);
        Map<String, Integer> completedOccurrences = calculatePhoneNumberOccurrences(completedTransactions);

        for (String phoneNumber : phoneNumbers) {
            int abortedCount = abortedOccurrences.getOrDefault(phoneNumber, 0);
            int committingCount = committingOccurrences.getOrDefault(phoneNumber, 0);
            int completedCount = completedOccurrences.getOrDefault(phoneNumber, 0);

            if (completedCount > 0) {
                phoneTransactionStatusMap.put(phoneNumber, TransactionStatus.COMMITTED);
            } else if (committingCount == abortedCount) {
                phoneTransactionStatusMap.put(phoneNumber, TransactionStatus.UNCOMMITTED);
            } else if (committingCount == abortedCount + 1) {
                phoneTransactionStatusMap.put(phoneNumber, TransactionStatus.COMMITTING);
            } else {
                throw new RuntimeException("Invalid transaction state for phone number: " + phoneNumber);
            }
        }

        return phoneTransactionStatusMap;
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


    /**
     * 指定されたInputStreamから全てのログタイプに基づいたトランザクションIDとキーのマップを作成する。
     *
     * @param inputStream ログデータのInputStream
     * @return 各ログタイプごとのトランザクションIDとキーのマップ
     * @throws RuntimeException 同じトランザクションIDがログに2回以上現れた場合
     */
    public Map<LogType, Map<String, String>> getAllTransactionIdKeyMaps(InputStream inputStream) throws RuntimeException {
        Map<LogType, Map<String, String>> allTransactionIdKeyMaps = new EnumMap<>(LogType.class);
        for (LogType logType : LogType.values()) {
            allTransactionIdKeyMaps.put(logType, new HashMap<>());
        }

        Pattern pattern = Pattern.compile("Transaction (committing|completed|aborted), tid = (.*?), txOption = .*?, key = (\\d{11})");

        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    LogType logType = LogType.valueOf(matcher.group(1).toUpperCase(Locale.ENGLISH));
                    String tid = matcher.group(2);
                    String key = matcher.group(3);

                    Map<String, String> transactionIdKeyMap = allTransactionIdKeyMaps.get(logType);
                    if (transactionIdKeyMap.containsKey(tid)) {
                        throw new RuntimeException("Duplicate transaction ID found: " + tid);
                    }
                    transactionIdKeyMap.put(tid, key);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return allTransactionIdKeyMaps;
    }

    private DatabaseSnapshot readDatabaseSnapshotFromFile(String prefix) {
        List<History> afterHistories = readListFromFile(prefix, History.class);
        List<Billing> afterBillings = readListFromFile(prefix, Billing.class);
        List<Contract> afterContracts = readListFromFile(prefix, Contract.class);
        return new DatabaseSnapshot(afterHistories, afterContracts, afterBillings);
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

    public enum TransactionStatus {
        COMMITTED,
        COMMITTING,
        UNCOMMITTED;
    }

    public enum LogType {
        COMMITTING,
        COMPLETED,
        ABORTED
    }

}
