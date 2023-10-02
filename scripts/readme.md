# 電話料金計算バッチ実行用スクリプト

このフォルダには、電話料金計算バッチを実行し必要な情報を必要な
情報を取得するためのスクリプトと設定ファイルを置いてあります。

これらのスクリプトは、tsurugiの開発者が電話料金計算バッチを実行したり、
CBで電話料金計算バッチを実行する際に使用することを想定しています。


## スクリプト

現在、以下のスクリプトが使用可能です。
* install.sh
  * 電話料金計算バッチをインストールする
* create_flame_graph.sh
  * 電話料金計算バッチを実行し、server(tsurugidb)と、client(電話料金計算バッチ)のフレームグラフを取得する
* env.sh
  * 他のスクリプト実行時に参照されるスクリプト
  * 環境依存の設定を記述する

## 各スクリプトの使用方法

* プロジェクトphone-bill-benchmarkの全ファイルを展開した状態で実行してください
* 以下のプロダクトがインストールされている必要があります
  * JDK11
    * 環境変数JAVA_HOMEが設定されていること
  * async profiler
    * 入手方法と、インストール方法
      * [async profiler](https://github.com/jvm-profiling-tools/async-profiler)
  * Tsurgui
  * git
    * project-tsurugiのリポジトリにアクセスできること
  * perfコマンド
    * Ubuntu標準のツール
    * https://github.com/project-tsurugi/sandbox-performance-tools/blob/master/docs/measurement.md
  * server側のフレームグラフ作成用のスクリプト
    * 以下の2つのパールスクリプトに実行権を与え、パスの通ったディレクトリに配置してください
      * stackcollapse-perf.pl
      * flamegraph.pl
    * 入手先
      * https://github.com/brendangregg/FlameGraph

### env.sh

以下の環境変数が定義されています。実行環境に応じて修正して使用してください。このファイルを修正するのではなく`$HOME/.phonebill` に環境変数
を設定することもできます。

* INSTALL_DIR
  * 電話料金計算バッチのインストールディレクトリ。指定されたディレクトリの下にphone-billというディレクトリが作成され、ディレクトリphone-billに電話料金計算バッチがインストールされる。
  * デフォルト値は`$HOME`
* TSURUGI_DIR
  * Tsurugiのインストールディレクトリ
  * Tsurugiにinstall.shでprefixに指定したディレクトリ+そのサブディレクトリを指定してください。
  * デフォルト値は`$HOME/tsurugi/tsurugi`
* ASYNC_PROFILER_DIR
  * async profilerのインストールディレクトリ
  * デフォルト値は `$HOME/async-profiler-2.8.3-linux-x64/`
* LOG_DIR
  * ログファイル等の出力先
    * tsurugidbのログ
    * 電話料金計算バッチのログ
    * server側のflame graph
    * client側のflame graph
  * デフォルト値は `$HOME/logs`
* TSURUGI_LOG_DIR
  * tsurugiのデータファイルの保存場所
  * デフォルト値は`$TSURUGI_DIR/var/data/log`
* TSURUGI_LOG_LEVEL
  * tsurugidbのログレベル
  * デフォルト値は30
  * 詳細なログが必要なときは50を指定する
  * TSURUGI_LOG_LEVELが既に設定済みの場合は`env.sh`, `$HOME/.phonebill`より既存の設定値を優先する。

### install.sh

このスクリプトを実行すると電話料金計算バッチをビルド、インストールアーカイブを作成し、
指定のディレクトリにインストールアーカイブを展開して電話料金計算バッチを新ストールします。

### create_flame_graph.sh

* このスクリプトを実行すると以下の処理が実行されます

1. OLTPサーバの起動
    * tsurugidbが動いている場合、強制終了する。
    * ログファイル、データベースファイルを削除する
    * tsurugidbを起動する
1. 電話料金計算バッチのテストデータの生成
1. perfコマンドによるtsurugidbのプロファイル情報の収集を開始
1. 電話料金計算バッチを実行
    * 同時にasync profilerによるプロファイル情報の収集を行う
    * バッチ終了時に、client側のフレームグラフが出力される
1. バッチ終了後にperfコマンドを停止し、収集したプロファイル情報を出力する
1. perfコマンドで収集したプロファイル情報からserver側のflame graphを生成する

* configファイルについて
  * 引数として電話料金計算バッチの設定ファイルを指定します。
  * configフォルダの下に置かれている設定ファイルを指定することを想定しています。
  * DBMSがipc接続のtsurugiの場合のみ検証しています。tcp接続のtsurugiでも恐らく動作します。PostgreSQL/Oracleについては何らかの対応が必要です。
  * configファイルのベース名は、LOG_DIRに出力されるログファイル等のファイル名の一部として使用されます。
  * ファイル名の命名規則
    * {OCC|LTX}-{CONTRACT|WHOLE}-T{n}.properties
      * OCC: 指定するTXタイプががOCC
      * LTX: 指定するTXタイプががLTX
      * CONTRACT: 1電話番号=1トランザクション
      * WHOLE: 1スレッドの処理全体が1トランザクション
      * n: スレッド数
* 出力ファイルについて
  * 以下のファイルがLOG_DIRで指定されたディレクトリに出力されます。`$BASENAME`はconfigファイルのファイル名のベース名です。
    * $BASSENAME-T1-server-fg.svg
      * サーバ側のflame graph
    * $BASSENAME-T1-client-fg.html
      * クライアント側のflame graph
    * $BASSENAME-T1-tsurugidb.log
      * tsurugidbのログ
    * $BASSENAME-T1-client.log
      * 電話料金計算バッチのログ
  
