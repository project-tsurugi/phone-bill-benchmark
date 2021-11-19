package com.example.nedo.app;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import com.example.nedo.app.billing.PhoneBill;
import com.example.nedo.multinode.NetworkIO.Message;
import com.example.nedo.multinode.client.CommandLineClient;
import com.example.nedo.multinode.client.OnlineAppClient;
import com.example.nedo.multinode.client.PhoneBillClient;
import com.example.nedo.multinode.server.Server;
import com.example.nedo.testdata.CreateTestData;
import com.example.nedo.testdata.CreateTestDataCsv;
import com.example.nedo.testdata.LoadTestDataCsvToOracle;
import com.example.nedo.testdata.LoadTestDataCsvToPostgreSql;
import com.example.nedo.testdata.TestDataStatistics;

public class Main {
	private static final Map<String, Command> COMMAND_MAP = new LinkedHashMap<>();
	static {
		addCommand("CreateTable", "Create tables", new CreateTable(), ArgType.CONFIG);
		addCommand("CreateTestData", "Create test data to database.", new CreateTestData(), ArgType.CONFIG);
		addCommand("PhoneBill", "Execute phone bill batch.", new PhoneBill(), ArgType.CONFIG);
		addCommand("ThreadBench", "Execute PhonBill with multiple thread counts", new ThreadBench(), ArgType.CONFIG);
		addCommand("OnlineAppBench", "Execute PhonBill with and without online applications.", new OnlineAppBench(),
				ArgType.CONFIG);
		addCommand("TestDataStatistics", "Create test data statistics without test data.", new TestDataStatistics(),
				ArgType.CONFIG);
		addCommand("CreateTestDataCsv", "Create test data to csv files.", new CreateTestDataCsv(), ArgType.CONFIG);
		addCommand("LoadTestDataCsvToOracle", "Load csv test data to oracle.", new LoadTestDataCsvToOracle(),
				ArgType.CONFIG);
		addCommand("LoadTestDataCsvToPostgreSql", "Load csv test data to PostgreSQL.",
				new LoadTestDataCsvToPostgreSql(), ArgType.CONFIG);
		addCommand("Server", "Execute the server process for multinode execution.", new Server(), ArgType.CONFIG);
		addCommand("PhoneBillClient", "Execute phone bill batch client for multienode execution.",
				new PhoneBillClient(), ArgType.HOST_AND_PORT);
		addCommand("OnlineAppClient", "Execute phone bill batch client for multienode execution.",
				new OnlineAppClient(), ArgType.HOST_AND_PORT);
		addCommand("Status", "Reports the execution status of client processes.",
				new CommandLineClient(Message.GET_CLUSTER_STATUS), ArgType.HOST_AND_PORT);
		addCommand("Shutdown", "Terminate all client processes and a server process.",
				new CommandLineClient(Message.SHUTDOWN_CLUSTER), ArgType.HOST_AND_PORT);
		addCommand("Start", "Start execution a phone bill batch and online applications.",
				new CommandLineClient(Message.START_EXECUTION), ArgType.HOST_AND_PORT);
	}

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.err.println("ERROR: No argument is specified.");
			usage();
			System.exit(1);
		}
		String cmd = args[0];
		Command command = COMMAND_MAP.get(cmd);
		if (command == null) {
			System.err.println("ERROR: Command '" + cmd + "' is not available.");
			usage();
			System.exit(1);
		}
		switch(command.argType) {
		case CONFIG:
			Config config = Config.getConfig(Arrays.copyOfRange(args, 1, args.length));
			command.executableCommand.execute(config);
			break;
		case HOST_AND_PORT:
			if (args.length == 1 || !args[1].contains(":")) {
				System.err.println("Host name and port number required");
				usage();
				System.exit(1);
			}
			String hostname = args[1].replaceAll(":.*", "");
			String port = args[1].replaceAll(".*:", "");
			command.executableCommand.execute(hostname, Integer.parseInt(port));
		}
	}

	private static void usage() {
		System.err.println();
		System.err.println("usage: run command [file]");
		System.err.println("  or:  run command hostname:port");
		System.err.println();
		System.err.println("Following commands can specify a filename of configuration file,");
		System.err.println("If not specified filename, the default value is used.");
		System.err.println();
		for(Command command: COMMAND_MAP.values()) {
			if (command.argType == ArgType.CONFIG) {
				System.err.println("  " + command.name+": " + command.description);
			}
		}
		System.err.println();
		System.err.println("Following commands must specify a hostname and port number of server.");
		System.err.println();
		for(Command command: COMMAND_MAP.values()) {
			if (command.argType == ArgType.HOST_AND_PORT) {
				System.err.println("  " + command.name+": " + command.description);
			}
		}
	}

	private static  class Command {
		String name;
		String description;
		ExecutableCommand executableCommand;
		ArgType argType;
	}

	private static enum ArgType {
		CONFIG,
		HOST_AND_PORT
	}

	private static void addCommand(String name, String description, ExecutableCommand instance, ArgType argType) {
		Command command = new Command();
		command.name = name;
		command.description = description;
		command.executableCommand = instance;
		command.argType = argType;
		COMMAND_MAP.put(name, command);
	}

}
