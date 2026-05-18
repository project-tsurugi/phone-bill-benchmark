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
package com.tsurugidb.benchmark.phonebill.testdata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.ExecutableCommand;
import com.tsurugidb.benchmark.phonebill.app.billing.PhoneBill;
import com.tsurugidb.benchmark.phonebill.app.billing.SimpleBillingCalculator;
import com.tsurugidb.benchmark.phonebill.app.billing.SimpleCallChargeCalculator;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.jdbc.Duration;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

public class CreateTestDataParquet extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(CreateTestDataParquet.class);
    private static final String DEFAULT_OUTPUT_DIR = "dump";
    private static final String OUTPUT_DIR_SYSPROP = "phone-bill.parquet.output.dir";
    private static final int HISTORY_RECORDS_PER_FILE = 1_000_000;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("Config filename required.");
        }
        Config config = Config.getConfig(args[0]);
        Path outputDir = args.length >= 2 ? Paths.get(args[1]) : Paths.get(DEFAULT_OUTPUT_DIR);
        new CreateTestDataParquet().execute(config, outputDir);
    }

    @Override
    public void execute(Config config) throws Exception {
        execute(config, Paths.get(System.getProperty(OUTPUT_DIR_SYSPROP, DEFAULT_OUTPUT_DIR)));
    }

    public void execute(Config config, Path outputDir) throws Exception {
        Files.createDirectories(outputDir);
        deleteIfExists(outputDir.resolve("contracts"));
        deleteIfExists(outputDir.resolve("history"));
        deleteIfExists(outputDir.resolve("billing"));

        Duration targetDuration = PhoneBill.toDuration(config.targetMonth);
        Date billingMonthStart = targetDuration.getStatDate();
        Date billingMonthEndInclusive = targetDuration.getEndDate();
        long billingMonthStartMillis = billingMonthStart.getTime();
        long nextMonthStartMillis = DateUtils.nextDate(billingMonthEndInclusive).getTime();

        Config effectiveConfig = createEffectiveGeneratorConfig(config);
        int phoneArraySize = getPhoneArraySize(effectiveConfig);
        BillingAccumulator accumulator = new BillingAccumulator(phoneArraySize);
        boolean calculateBilling = !config.onlineOnly;

        int seed = config.randomSeed;
        ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
        TestDataGenerator generator = new TestDataGenerator(config, new Random(seed), accessor);

        writeContracts(config, outputDir, generator, accumulator, billingMonthStart, billingMonthEndInclusive,
                calculateBilling);
        writeHistory(config, outputDir, generator, accumulator, billingMonthStartMillis, nextMonthStartMillis,
                calculateBilling);
        if (calculateBilling) {
            writeBilling(effectiveConfig, outputDir, accumulator, billingMonthStart);
        } else {
            LOG.info("Skipped billing parquet generation because online.only=true.");
        }
    }

    private void writeContracts(Config config, Path outputDir, TestDataGenerator generator,
            BillingAccumulator accumulator, Date billingMonthStart, Date billingMonthEndInclusive,
            boolean calculateBilling) throws IOException {
        Path outputFile = outputDir.resolve("contracts").resolve("part-00000.parquet");
        long startTime = System.currentTimeMillis();
        try (PhoneBillParquetWriter<Contract> writer = PhoneBillParquetWriter.contracts(outputFile)) {
            for (long n = 0; n < config.numberOfContractsRecords; n++) {
                Contract contract = generator.getNewContract();
                writer.write(contract);
                if (calculateBilling && isBillingTarget(contract, billingMonthStart, billingMonthEndInclusive)) {
                    accumulator.addTarget(phoneIndex(contract.getPhoneNumber()));
                }
            }
        }
        long elapsedTime = System.currentTimeMillis() - startTime;
        LOG.info(String.format("%,d records generated to contracts parquet in %,.3f sec",
                config.numberOfContractsRecords, elapsedTime / 1000d));
    }

    private void writeHistory(Config config, Path outputDir, TestDataGenerator generator,
            BillingAccumulator accumulator, long billingMonthStartMillis, long nextMonthStartMillis,
            boolean calculateBilling) {
        long startTime = System.currentTimeMillis();
        generator.generateHistory(params -> generator.new HistoryWriter() {
            private PhoneBillParquetWriter<History> writer;
            private final SimpleCallChargeCalculator callChargeCalculator = new SimpleCallChargeCalculator();

            @Override
            void init() throws IOException {
                Path outputFile = outputDir.resolve("history").resolve(String.format("part-%05d.parquet", params.taskId));
                writer = PhoneBillParquetWriter.history(outputFile);
            }

            @Override
            void write(History history) throws IOException {
                if (calculateBilling) {
                    Integer charge = getCharge(history);
                    history.setCharge(charge);
                }
                writer.write(history);
            }

            @Override
            void cleanup() throws IOException {
                if (writer != null) {
                    writer.close();
                }
            }

            private Integer getCharge(History history) {
                if (history.getDf() != 0) {
                    return null;
                }
                long startTime = history.getStartTime().getTime();
                if (startTime < billingMonthStartMillis || nextMonthStartMillis <= startTime) {
                    return null;
                }
                int payerIndex;
                if ("C".equals(history.getPaymentCategorty())) {
                    payerIndex = phoneIndex(history.getCallerPhoneNumber());
                } else if ("R".equals(history.getPaymentCategorty())) {
                    payerIndex = phoneIndex(history.getRecipientPhoneNumber());
                } else {
                    return null;
                }
                if (!accumulator.isTarget(payerIndex)) {
                    return null;
                }
                int charge = callChargeCalculator.calc(history.getTimeSecs());
                accumulator.addCharge(payerIndex, charge);
                return charge;
            }
        }, HISTORY_RECORDS_PER_FILE);
        long elapsedTime = System.currentTimeMillis() - startTime;
        LOG.info(String.format("%,d records generated to history parquet in %,.3f sec",
                config.numberOfHistoryRecords, elapsedTime / 1000d));
    }

    private void writeBilling(Config config, Path outputDir, BillingAccumulator accumulator, Date billingMonthStart)
            throws IOException {
        Path outputFile = outputDir.resolve("billing").resolve("part-00000.parquet");
        String batchExecId = UUID.randomUUID().toString();
        PhoneNumberGenerator phoneNumberGenerator = new PhoneNumberGenerator(config);
        long count = 0;
        long startTime = System.currentTimeMillis();
        try (PhoneBillParquetWriter<Billing> writer = PhoneBillParquetWriter.billing(outputFile)) {
            for (int phoneIndex = accumulator.nextTarget(0); phoneIndex >= 0;
                    phoneIndex = accumulator.nextTarget(phoneIndex + 1)) {
                int meteredCharge = accumulator.getCharge(phoneIndex);
                SimpleBillingCalculator calculator = new SimpleBillingCalculator();
                calculator.init();
                calculator.addCallCharge(meteredCharge);

                Billing billing = new Billing();
                billing.setPhoneNumber(phoneNumberGenerator.getPhoneNumber(phoneIndex));
                billing.setTargetMonth(billingMonthStart);
                billing.setBasicCharge(calculator.getBasicCharge());
                billing.setMeteredCharge(calculator.getMeteredCharge());
                billing.setBillingAmount(calculator.getBillingAmount());
                billing.setBatchExecId(batchExecId);
                writer.write(billing);
                count++;
            }
        }
        long elapsedTime = System.currentTimeMillis() - startTime;
        LOG.info(String.format("%,d records generated to billing parquet in %,.3f sec", count, elapsedTime / 1000d));
    }

    private static boolean isBillingTarget(Contract contract, Date billingMonthStart, Date billingMonthEndInclusive) {
        return contract.getStartDate().getTime() <= billingMonthEndInclusive.getTime()
                && (contract.getEndDate() == null || contract.getEndDate().getTime() >= billingMonthStart.getTime());
    }

    private static int getPhoneArraySize(Config config) {
        long size = (long) config.numberOfContractsRecords + ContractInfoReader.getContractBlockSize(config) + 1;
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Too many contracts for parquet-direct phone index array: " + size);
        }
        return (int) size;
    }

    private static int phoneIndex(String phoneNumber) {
        long value = Long.parseLong(phoneNumber);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Phone number is too large for parquet-direct index: " + phoneNumber);
        }
        return (int) value;
    }

    private static Config createEffectiveGeneratorConfig(Config config) {
        Config effectiveConfig = config.clone();
        if (config.enableUniformContractDuration) {
            effectiveConfig.duplicatePhoneNumberRate = 0;
            effectiveConfig.expirationDateRate = 0;
            effectiveConfig.noExpirationDateRate = ContractInfoReader.getContractBlockSize(config);
        }
        return effectiveConfig;
    }

    private static void deleteIfExists(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (var stream = Files.walk(path)) {
            for (Path p: stream.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        }
    }

    private static class BillingAccumulator {
        private final BitSet targets;
        private final AtomicIntegerArray meteredCharges;

        BillingAccumulator(int size) {
            this.targets = new BitSet(size);
            this.meteredCharges = new AtomicIntegerArray(size);
        }

        synchronized void addTarget(int phoneIndex) {
            targets.set(phoneIndex);
        }

        boolean isTarget(int phoneIndex) {
            return targets.get(phoneIndex);
        }

        int nextTarget(int fromIndex) {
            return targets.nextSetBit(fromIndex);
        }

        void addCharge(int phoneIndex, int charge) {
            meteredCharges.addAndGet(phoneIndex, charge);
        }

        int getCharge(int phoneIndex) {
            return meteredCharges.get(phoneIndex);
        }
    }
}
