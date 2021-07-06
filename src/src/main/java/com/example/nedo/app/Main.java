package com.example.nedo.app;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import com.example.nedo.app.billing.PhoneBill;
import com.example.nedo.testdata.CreateTestData;
import com.example.nedo.testdata.TestDataStatistics;

public class Main {
	private static final Map<String, Command> COMMAND_MAP = new LinkedHashMap<>();
	static {
		addCommand("CreateTable","Create tables", new CreateTable());
		addCommand("CreateTestData","Create test data", new CreateTestData());
		addCommand("PhoneBill", "Execute phone bill batch.", new PhoneBill());
		addCommand("ThreadBench", "Execute PhonBill with multiple thread counts", new ThreadBench());
		addCommand("OnlineAppBench", "Execute PhonBill with and without online appications.", new OnlineAppBench());
		addCommand("TestDataStatistics", "Create test data statistics without testdata.", new TestDataStatistics());
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
		Config config = Config.getConfig(Arrays.copyOfRange(args, 1, args.length));
		command.executableCommand.execute(config);
	}


	private static void usage() {
		System.err.println();
		System.err.println("USAGE: run COMMAND [FILE]");
		System.err.println();
		System.err.println("COMMAND: Following commands available");
		System.err.println();
		for(Command command: COMMAND_MAP.values()) {
			System.err.println("  " + command.name+" : " + command.description);
		}
		System.err.println();
		System.err.println("FILE: The filename of the configuration file, if not specified, the default value is used.");
		System.err.println();
	}

	private static  class Command {
		String name;
		String description;
		ExecutableCommand executableCommand;
	}

	private static void addCommand(String name, String description, ExecutableCommand instance) {
		Command command = new Command();
		command.name = name;
		command.description = description;
		command.executableCommand = instance;
		COMMAND_MAP.put(name, command);
	}

}
