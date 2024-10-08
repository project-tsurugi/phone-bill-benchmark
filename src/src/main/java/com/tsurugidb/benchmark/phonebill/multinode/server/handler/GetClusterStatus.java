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
package com.tsurugidb.benchmark.phonebill.multinode.server.handler;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;

import com.tsurugidb.benchmark.phonebill.multinode.server.ClientInfo;
import com.tsurugidb.benchmark.phonebill.multinode.server.Server.ServerTask;

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
		String format = "%-13s %-10s %-15s %-9s %s";

		String header = String.format(format, "Start", "Type", "Node", "Status", "Message from client");
		int maxLen = header.length();
		for (ClientInfo info : clientInfos) {
			String startTime = DateTimeFormatter.ofPattern("HH:mm:ss")
					.format(LocalDateTime.ofInstant(info.getStart(), ZoneId.systemDefault()));
			String line = String.format(format, startTime, info.getType(), info.getNode(), info.getStatus(),
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
