package com.example.nedo.multinode.server.handler;

import com.example.nedo.multinode.server.Server.ServerTask;

public class InitCommandLineClient extends MessageHandlerBase {
	/**
	 * @param server このハンドラを呼び出すServerTask
	 */
	public InitCommandLineClient(ServerTask task) {
		super(task);
	}
}
