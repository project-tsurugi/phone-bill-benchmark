package com.example.nedo.multinode.server.handler;

import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;

import com.example.nedo.multinode.server.ClientInfo;
import com.example.nedo.multinode.server.Server.ServerTask;

public class GetClusterStatus extends MessageHandlerBase {
	public GetClusterStatus(ServerTask task) {
		super(task);
	}

	@Override
	public String getResponseString() {
		return createStatusReport(getTask().getClientInfos());
	}

	/**
	 * @return
	 */
	static String createStatusReport(Set<ClientInfo> clientInfos) {
		Set<String> lines = new TreeSet<String>();
		String format = "%-10s %-15s %-13s %s";

		String header = String.format(format, "Type", "Node", "Status", "Message from client");
		int maxLen = header.length();
		for(ClientInfo info: clientInfos) {
			String line = String.format(format, info.getType(), info.getNode(), info.getStatus(),
					info.getMessageFromClient());
			if (maxLen < line.length()) {
				maxLen = line.length();
			}
			lines.add(line);
		}
		StringBuffer sb = new StringBuffer();
		sb.append("\n");
		sb.append(header);
		sb.append("\n");
		sb.append(StringUtils.repeat('-', maxLen));
		sb.append("\n");
		for(String line : lines) {
			sb.append(line);
			sb.append("\n");
		}
		return sb.toString();
	}
}
