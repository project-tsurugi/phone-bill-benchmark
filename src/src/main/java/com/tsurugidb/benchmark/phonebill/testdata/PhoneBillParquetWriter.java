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
import java.sql.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;

class PhoneBillParquetWriter<T> implements AutoCloseable {
    private static final MessageType CONTRACTS_SCHEMA = MessageTypeParser.parseMessageType(
            "message schema {"
            + " optional binary phone_number (UTF8);"
            + " optional int32 start_date (DATE);"
            + " optional int32 end_date (DATE);"
            + " optional binary charge_rule (UTF8);"
            + "}");
    private static final MessageType HISTORY_SCHEMA = MessageTypeParser.parseMessageType(
            "message schema {"
            + " optional binary caller_phone_number (UTF8);"
            + " optional binary recipient_phone_number (UTF8);"
            + " optional binary payment_category (UTF8);"
            + " optional int64 start_time (TIMESTAMP(NANOS,false));"
            + " optional int32 time_secs (INTEGER(32,true));"
            + " optional int32 charge (INTEGER(32,true));"
            + " optional int32 df (INTEGER(32,true));"
            + "}");
    private static final MessageType BILLING_SCHEMA = MessageTypeParser.parseMessageType(
            "message schema {"
            + " optional binary phone_number (UTF8);"
            + " optional int32 target_month (DATE);"
            + " optional int32 basic_charge (INTEGER(32,true));"
            + " optional int32 metered_charge (INTEGER(32,true));"
            + " optional int32 billing_amount (INTEGER(32,true));"
            + " optional binary batch_exec_id (UTF8);"
            + "}");

    private final ParquetWriter<Group> writer;
    private final SimpleGroupFactory factory;
    private final RowMapper<T> mapper;

    static PhoneBillParquetWriter<Contract> contracts(Path outputFile) throws IOException {
        return new PhoneBillParquetWriter<>(outputFile, CONTRACTS_SCHEMA, (factory, contract) -> {
            Group group = factory.newGroup()
                    .append("phone_number", contract.getPhoneNumber())
                    .append("start_date", toEpochDay(contract.getStartDate()))
                    .append("charge_rule", contract.getRule());
            if (contract.getEndDate() != null) {
                group.append("end_date", toEpochDay(contract.getEndDate()));
            }
            return group;
        });
    }

    static PhoneBillParquetWriter<History> history(Path outputFile) throws IOException {
        return new PhoneBillParquetWriter<>(outputFile, HISTORY_SCHEMA, (factory, history) -> {
            Group group = factory.newGroup()
                    .append("caller_phone_number", history.getCallerPhoneNumber())
                    .append("recipient_phone_number", history.getRecipientPhoneNumber())
                    .append("payment_category", history.getPaymentCategorty())
                    .append("start_time", history.getStartTime().getTime() * 1_000_000L)
                    .append("time_secs", history.getTimeSecs())
                    .append("df", history.getDf());
            if (history.getCharge() != null) {
                group.append("charge", history.getCharge());
            }
            return group;
        });
    }

    static PhoneBillParquetWriter<Billing> billing(Path outputFile) throws IOException {
        return new PhoneBillParquetWriter<>(outputFile, BILLING_SCHEMA, (factory, billing) -> factory.newGroup()
                .append("phone_number", billing.getPhoneNumber())
                .append("target_month", toEpochDay(billing.getTargetMonth()))
                .append("basic_charge", billing.getBasicCharge())
                .append("metered_charge", billing.getMeteredCharge())
                .append("billing_amount", billing.getBillingAmount())
                .append("batch_exec_id", billing.getBatchExecId()));
    }

    private PhoneBillParquetWriter(Path outputFile, MessageType schema, RowMapper<T> mapper) throws IOException {
        Files.createDirectories(outputFile.getParent());
        this.factory = new SimpleGroupFactory(schema);
        this.mapper = mapper;
        this.writer = ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(outputFile.toUri()))
                .withConf(new Configuration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();
    }

    void write(T row) throws IOException {
        writer.write(mapper.map(factory, row));
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    private static int toEpochDay(Date date) {
        return (int) date.toLocalDate().toEpochDay();
    }

    private interface RowMapper<T> {
        Group map(SimpleGroupFactory factory, T row);
    }
}
