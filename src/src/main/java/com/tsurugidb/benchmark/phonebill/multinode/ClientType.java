package com.tsurugidb.benchmark.phonebill.multinode;

/**
 * クライアントの種別を定義する列挙型
 */
public enum ClientType {
	ONLINE_APP,// オンラインアプリ
	PHONEBILL,// 料金計算バッチ
	COMMAND_LINE,//コマンドラインクライアント
}
