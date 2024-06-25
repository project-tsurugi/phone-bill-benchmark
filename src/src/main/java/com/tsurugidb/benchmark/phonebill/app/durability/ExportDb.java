package com.tsurugidb.benchmark.phonebill.app.durability;

import java.io.BufferedOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;

public class ExportDb {

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            usage();
            System.exit(1);
        }

        String configFilePath = args[0];
        String exportFilePrefix = args[1];
        boolean exportAsText = false;

        if (args.length >= 3 && args[2].equals("--text")) {
            exportAsText = true;
        }

        new ExportDb().exec(configFilePath, exportFilePrefix, exportAsText);
    }

    private static void usage() {
        System.out.println("Usage: java ExportDb <config-file-path> <export-file-prefix> [--text]");
    }

    private void exec(String configFilePath, String exportFilePrefix, boolean exportAsText) throws IOException {
        Config config = Config.getConfig(configFilePath);

        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            manager.execute(TxOption.of(), () -> {
                if (exportAsText) {
                    writeListToTextFile(manager.getHistoryDao()::getHistories, exportFilePrefix, History.class);
                    writeListToTextFile(manager.getContractDao()::getContracts, exportFilePrefix, Contract.class);
                    writeListToTextFile(manager.getBillingDao()::getBillings, exportFilePrefix, Billing.class);
                } else {
                    writeListToFile(manager.getHistoryDao()::getHistories, exportFilePrefix, History.class);
                    writeListToFile(manager.getContractDao()::getContracts, exportFilePrefix, Contract.class);
                    writeListToFile(manager.getBillingDao()::getBillings, exportFilePrefix, Billing.class);
                }
            });
        }
    }

    private <T extends Serializable> void writeListToFile(Supplier<List<T>> supplier, String filePrefix,
            Class<T> clazz) {
        Path path = FileNameUtil.buildFilePathWithClass(filePrefix, clazz);
        List<T> list = supplier.get();
        try (ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
            out.writeObject(list);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private <T extends Serializable> void writeListToTextFile(Supplier<List<T>> supplier, String filePrefix,
            Class<T> clazz) {
        Path path = FileNameUtil.buildFilePathWithClass(filePrefix, clazz);
        List<T> list = supplier.get();
        Collections.sort(list, (a, b) -> a.toString().compareTo(b.toString()));
        try (FileWriter writer = new FileWriter(path.toFile())) {
            for (T record : list) {
                writer.write(record.toString() + "\n"); // Assuming T has a meaningful toString method
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
