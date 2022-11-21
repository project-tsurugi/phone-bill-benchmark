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
  * 電話料金計算バッチを実行し、server(tateyama server)と、client(電話料金計算バッチ)のフレームグラフを取得する。
* env.sh
  * 他のスクリプト実行時に参照されるスクリプト
  * 環境依存の設定を記述する

## 各スクリプトの使用方法

* プロジェクトphone-bill-benchmarkの全ファイルを展開した状態で実行してください。
* 以下のプロダクトがインストールされている必要があります。
  * JDK11
    * 環境変数JAVA_HOMEが設定されていること
  * async profiler
    * 入手方法と、インストール方法
      * [async profiler](https://github.com/jvm-profiling-tools/async-profiler)
  * tsurugi-distribution
    * ここから入手したtsurugi-distributionをインストール
      * https://github.com/project-tsurugi/tsurugi-distribution
    * 次のバージョンで動作確認済み
      * tsurugi-0.202210281811-alpha
      * tsurugi-0.202211041808-alpha
      * tsurugi-0.202211110938-alpha
  * git
    * project-tsurugiのリポジトリにアクセスできること
  * perfコマンド
    * Ubuntu標準のツールです
    * https://github.com/project-tsurugi/sandbox-performance-tools/blob/master/docs/measurement.md
  * server側のフレームグラフ作成用のスクリプト
    * 以下の2つのパールスクリプトに実行権を与え、パスの通ったディレクトリに配置してください。
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
  * tsurugi-distributionのインストールディレクトリ
  * tsurugi-distributionのインストール時にinstall.shでprefixに指定したディレクトリ+そのサブディレクトリを指定してください。
  * デフォルト値は`$HOME/tsurugi/tsurugi`
* ASYNC_PROFILER_DIR
  * async profilerのインストールディレクトリ
  * デフォルト値は `$HOME/async-profiler-2.8.3-linux-x64/`
* LOG_DIR
  * ログファイル等の出力先
    * tateyama-serverのログ
    * 電話料金計算バッチのログ
    * server側のflame graph
    * client側のflame graph
  * デフォルト値は `$HOME/logs`

### install.sh

このスクリプトを実行すると電話料金計算バッチをビルド、インストールアーカイブを作成し、
指定のディレクトリにインストールアーカイブを展開して電話料金計算バッチを新ストールします。

### create_flame_graph.sh

* このスクリプトを実行すると以下の処理が実行されます

1. OLTPサーバの起動
    * tateyama-serverが動いている場合、強制終了する。
    * ログファイル、データベースファイルを削除する
    * tateyama-serverを起動する
1. 電話料金計算バッチのテストデータの生成
1. perfコマンドによるtateyama-serverのプロファイル情報の収集を開始
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
    * $BASSENAME-T1-tateyama-server.log
      * tateyama serverのログ
    * $BASSENAME-T1-client.log
      * 電話料金計算バッチのログ
  
## その他、課題など

* テストデータのデータ量
  * 現在のデータ量だとdbs44で1スレッドの実行で25～50秒程度、スレッド数を増やした場合やtsurugiの性能向上により測定時間が短すぎる場合はデータ量を増やした方が良い。
* スレッド数
  * 現在スレッド数=1でのみ動作確認済み
  * issue#99のため複数スレッドでの動作確認ができない 
    * issue#99がクローズされたのでテストする予定
* create_flame_graph.shの処理時間は、電話料金計算バッチ以外の処理を多数含むため、電話料金計算バッチの処理時間の目安としても使用できない。現時点では、次のログから処理時間を抽出する必要がある。
```
18:03:08.027 [main] INFO  c.t.b.p.app.billing.PhoneBill - Billings calculated in 42.373 sec 
```
* TXのリトライ回数出力されない
  * 別のテストプログラムでは出力しているが、add hocな対応をしていたので用整理、整理後出力するようにする。
* CBで使用する場合に、処理時間やTXのリトライ回数などの情報をどのように出力、蓄積するのが良いのか
* スレッド数を増やした場合に、tateyama-serverの設定を変更する必要があるかもしれない
  * `etc/phone-bill.ini` の thread_pool_size
* perfコマンドが出力するプロファイル情報のファイルサイズが非常に大きい
  * 現状で20GByte程度、処理時間が延びたりスレッド数を増やしたりした場合に問題が起きそう。
    * もっとも簡単な対応方法は、サンプリング間隔を延ばすこと => 測定精度とのトレードオフ
    * 他に方法がないか要調査
* `create_flame_graph.sh` 実行時に他のtateyama-serverが動いているせいで、tateyama-serverの起動に失敗することがある。この場合、原因となるtateyama-server停止後に`create_flame_graph.sh` を実行すれば良い。
* `create_flame_graph.sh`の出力ファイルは実行ごとに上書きされます。
