package com.tsurugidb.benchmark.phonebill.app;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import com.tsurugidb.benchmark.phonebill.app.billing.PhoneBill;
import com.tsurugidb.benchmark.phonebill.multinode.NetworkIO.Message;
import com.tsurugidb.benchmark.phonebill.multinode.client.CommandLineClient;
import com.tsurugidb.benchmark.phonebill.multinode.client.OnlineAppClient;
import com.tsurugidb.benchmark.phonebill.multinode.client.PhoneBillClient;
import com.tsurugidb.benchmark.phonebill.multinode.server.Server;
import com.tsurugidb.benchmark.phonebill.testdata.CreateTestData;
import com.tsurugidb.benchmark.phonebill.testdata.CreateTestDataCsv;
import com.tsurugidb.benchmark.phonebill.testdata.LoadTestDataCsvToOracle;
import com.tsurugidb.benchmark.phonebill.testdata.LoadTestDataCsvToPostgreSql;
import com.tsurugidb.benchmark.phonebill.testdata.TestDataStatistics;

public class Main {
	private static final Map<String, Command> COMMAND_MAP = new LinkedHashMap<>();
	static {
		addCommand("CreateTable", "Create tables",  CreateTable.class, ArgType.CONFIG);
		addCommand("CreateTestData", "Create test data to database.", CreateTestData.class, ArgType.CONFIG);
		addCommand("PhoneBill", "Execute phone bill batch.", PhoneBill.class, ArgType.CONFIG);
		addCommand("ThreadBench", "Execute PhonBill with multiple thread counts", ThreadBench.class, ArgType.CONFIG);
		addCommand("OnlineAppBench", "Execute PhonBill with and without online applications.", OnlineAppBench.class,
				ArgType.CONFIG);
		addCommand("TestDataStatistics", "Create test data statistics without test data.", TestDataStatistics.class,
				ArgType.CONFIG);
		addCommand("CreateTestDataCsv", "Create test data to csv files.", CreateTestDataCsv.class, ArgType.CONFIG);
		addCommand("LoadTestDataCsvToOracle", "Load csv test data to oracle.", LoadTestDataCsvToOracle.class,
				ArgType.CONFIG);
		addCommand("LoadTestDataCsvToPostgreSql", "Load csv test data to PostgreSQL.",
				LoadTestDataCsvToPostgreSql.class, ArgType.CONFIG);
		addCommand("Server", "Execute the server process for multinode execution.", Server.class, ArgType.CONFIG);
		addCommand("PhoneBillClient", "Execute phone bill batch client for multienode execution.",
				PhoneBillClient.class, ArgType.HOST_AND_PORT);
		addCommand("OnlineAppClient", "Execute phone bill batch client for multienode execution.",
				OnlineAppClient.class, ArgType.HOST_AND_PORT);
		addCommand("Status", "Reports the execution status of client processes.",
				CommandLineClient.class, Message.GET_CLUSTER_STATUS, ArgType.HOST_AND_PORT);
		addCommand("Start", "Start execution a phone bill batch and online applications.",
				CommandLineClient.class, Message.START_EXECUTION, ArgType.HOST_AND_PORT);
		addCommand("Shutdown", "Terminate all client processes and a server process.",
				CommandLineClient.class, Message.SHUTDOWN_CLUSTER, ArgType.HOST_AND_PORT);
	}

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.err.println("ERROR: No argument is specified.");
			usage();
		}
		String cmd = args[0];
		Command command = COMMAND_MAP.get(cmd);
		if (command == null) {
			System.err.println("ERROR: Command '" + cmd + "' is not available.");
			usage();
		}

		ExecutableCommand executableCommand;
		if (command.constructorArg == null) {
			executableCommand = command.clazz.getConstructor().newInstance();
		} else {
			executableCommand = command.clazz.getConstructor(Message.class).newInstance(command.constructorArg);
		}


		switch(command.argType) {
		case CONFIG:
			try {
				Config config = Config.getConfig(false);
				executableCommand.execute(config);
			} catch (RuntimeException | IOException e) {
				e.printStackTrace();
				usage();
			}
			break;
		case HOST_AND_PORT:
			if (args.length == 1 || !args[1].contains(":")) {
				System.err.println("Host name and port number required");
				usage();
				System.exit(1);
			}
			String hostname = args[1].replaceAll(":.*", "");
			String port = args[1].replaceAll(".*:", "");
			executableCommand.execute(hostname, Integer.parseInt(port));
		}
	}


	private static void usage() {
		System.err.println();
		System.err.println("usage: run command");
		System.err.println("  or:  run command hostname:port");
		System.err.println();
		System.err.println("The following command requires a property file to be specified in the JAVA_OPTS");
		System.err.println("environment variable in the form -Dproperty=property-file-path.");
		System.err.println();
		for(Command command: COMMAND_MAP.values()) {
			if (command.argType == ArgType.CONFIG) {
				System.err.println("  " + command.name+": " + command.description);
			}
		}
		System.err.println();
		System.err.println("The following commands must specify a hostname and port number of server.");
		System.err.println();
		for(Command command: COMMAND_MAP.values()) {
			if (command.argType == ArgType.HOST_AND_PORT) {
				System.err.println("  " + command.name+": " + command.description);
			}
		}
		System.exit(1);
	}

	private static  class Command {
		String name;
		String description;
		Class<? extends ExecutableCommand> clazz;
		Message constructorArg;
		ArgType argType;
	}

	private static enum ArgType {
		CONFIG,
		HOST_AND_PORT
	}

	private static void addCommand(String name, String description, Class<? extends ExecutableCommand> clazz, ArgType argType) {
		addCommand(name, description, clazz, null, argType);
	}

	private static void addCommand(String name, String description, Class<? extends ExecutableCommand> clazz, Message constructorArg, ArgType argType) {
		Command command = new Command();
		command.name = name;
		command.description = description;
		command.clazz = clazz;
		command.constructorArg = constructorArg;
		command.argType = argType;
		COMMAND_MAP.put(name, command);
	}

}
