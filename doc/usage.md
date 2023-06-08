# サンプルバッチの使用方法


## ビルド環境と実行環境

以下の環境で開発、動作確認を行っています。同等環境を用意願います。

* OS
 - Ubuntu 20.04LTS
* JDK
  - openjdk 11.0.7
* DBMS
  - PostgreSQL 12
  - Oracle Database 19c Enterprise Edition
  - Tsurugi

## ビルドとインストール

### 認証情報の設定

GitHubのアクセスに必要な認証情報を設定

* 環境変数GPR_USERにGitHubのユーザ名を設定
  * project-tsurugiに対するアクセス権のあるユーザを指定
* 環境変数GPR_KEYにGitHubのパーソナルアクセストークンを設定
  * scopeに `repo`, `read:packages`を含むパーソナルアクセストークンが必要

### ソースコードをgithubから取得

```
git clone git@github.com:project-tsurugi/phone-bill-benchmark.git
```

### ビルド

```
cd src
./gradlew clean distTar
```
ビルドに成功すると、インストール用のアーカイブ`build/distributions/phone-bill.tar.gz`が生成される。

### インストール

実行環境の任意のディレクトリで、生成された`phone-bill.tar.gz` を展開してインストールする。

```
tar xf phone-bill.tar.gz
```

## 実行

スクリプト`phone-bill/bin/run`でサンプルバッチを起動する。下の例では、引数を与えて
いないので、エラーとなりusageが表示される。

```
$ ./phone-bill/bin/run
ERROR: No argument is specified.

usage: run command [file]
  or:  run command hostname:port

Following commands can specify a filename of configuration file,
If not specified filename, the default value is used.

  CreateTable: Create tables
  CreateTestData: Create test data to database.
  PhoneBill: Execute phone bill batch.
  ThreadBench: Execute PhonBill with multiple thread counts
  OnlineAppBench: Execute PhonBill with and without online applications.
  TestDataStatistics: Create test data statistics without test data.
  CreateTestDataCsv: Create test data to csv files.
  LoadTestDataCsvToOracle: Load csv test data to oracle.
  LoadTestDataCsvToPostgreSql: Load csv test data to PostgreSQL.
  Server: Execute the server process for multinode execution.

Following commands must specify the hostname and port number of server.

  PhoneBillClient: Execute phone bill batch client for multienode execution.
  OnlineAppClient: Execute phone bill batch client for multienode execution.
  Status: Reports the execution status of client processes.
  Start: Start execution a phone bill batch and online applications.
  Shutdown: Terminate all client processes and a server process.
```

## 設定ファイル

設定ファイルの例: デフォルトの設定値

```
# 料金計算に関するパラメータ
target.month=2020-12-01

# 契約マスタ生成に関するパラメータ
number.of.contracts.records=1000
duplicate.phone.number.rate=10
expiration.date.rate=30
no.expiration.date.rate=50
min.date=2010-11-11
max.date=2021-03-01

# 通話履歴生成に関するパラメータ
number.of.history.records=1000
recipient.phone.number.distribution=UNIFORM
recipient.phone.number.scale=3.0
recipient.phone.number.shape=18.0
caller.phone.number.distribution=UNIFORM
caller.phone.number.scale=3.0
caller.phone.number.shape=18.0
call.time.distribution=UNIFORM
call.time.scale=4.5
call.time.shape=1.5
max.call.time.secs=3600
statistics.output.dir=
history.min.date=2020-11-01
history.max.date=2021-01-10

# JDBCに関するパラメータ
url=jdbc:postgresql://127.0.0.1/phonebill
user=phonebill
password=phonebill
isolation.level=READ_COMMITTED
# DBMSタイプ
dbms.type=POSTGRE_SQL_JDBC


# オンラインアプリケーションに関するパラメータ
master.update.records.per.min=0
master.update.thread.count=1
master.delete.insert.records.per.min=0
master.delete.insert.thread.count=1
history.update.records.per.min=0
history.update.thread.count=1
history.insert.transaction.per.min=0
history.insert.records.per.transaction=1
history.insert.thread.count=1
skip.database.access=false

# スレッドに関するパラメータ
thread.count=1
shared.connection=true

# CSVに関するパラメータ
csv.dir=/var/lib/csv
max.number.of.lines.history.csv=1000000

# Oracle固有のパラメータ
oracle.initrans=0
oracle.sql.loader.path=sqlldr
oracle.sql.loader.sid=
oracle.create.index.option=nologging parallel 32

# Iceaxe固有のパラメータ
transaction.option=OCC
use.prepared.tables=false

# その他のパラメータ
random.seed=0
transaction.scope=WHOLE
listen.port=0
```

### 料金計算に関するパラメータ

* target.month
  - 料金計算対象の年月の1日を、yyyy-mm-ddの形式で指定する。

### 契約マスタのテストデータ生成に関するパラメータ
* number.of.contracts.records
  - 契約マスタのレコード数
* duplicate.phone.number.rate
  - 複数の契約を持つ電話番号の割合
* expiration.date.rate
  - 契約終了日を持つ契約の割合
* no.expiration.date.rate
  - 契約終了日を持たない契約の割合
* min.date, max.date
  - 契約開始日と契約終了日がmin.date～max.dateの範囲に収まるようにテストデータを生成する。yyyy-mm-ddの形式で指定する。

### 通話履歴生成に関するパラメータ

* number.of.history.records
  - 通話履歴のレコード数
* recipient.phone.number.distribution
  - 受信者電話番号生成時に使用する分布関数、`UNIFORM`と`LOGNORMAL`のみが指定可能
* recipient.phone.number.scale
  - 受信者電話番号生成時に使用する分布関数の指定が`LOGNORMAL`の場合に使用するscaleの値
* recipient.phone.number.shape
  - 受信者電話番号生成時に使用する分布関数の指定が`LOGNORMAL`の場合に使用するshapeの値
* caller.phone.number.distribution
  - 発信者電話番号生成時に使用する分布関数、`UNIFORM`と`LOGNORMAL`のみが指定可能
* caller.phone.number.scale
  - 発信者電話番号生成時に使用する分布関数の指定が`LOGNORMAL`の場合に使用するscaleの値
* caller.phone.number.shape
  - 発信者電話番号生成時に使用する分布関数の指定が`LOGNORMAL`の場合に使用するshapeの値
* call.time.distribution
  - 通話時間生成時に使用する分布関数、`UNIFORM`と`LOGNORMAL`のみが指定可能
* call.time.scale
  - 通話時間生成時に使用する分布関数の指定が`LOGNORMAL`の場合に使用するscaleの値
* call.time.shape
  - 通話時間生成時に使用する分布関数の指定が`LOGNORMAL`の場合に使用するshapeの値
* max.call.time.secs
  - 通話時間生成時の最大値
* statistics.output.dir
  - 統計情報を出力するディレクトリ、ここで指定したディレクトリに統計情報ファイルを出力する。このパラメータを指定しない場合は統計情報ファイルを出力しない。
* history.min.date
  - 生成する履歴データの通話開始日の最小値
* history.max.date
  - 生成する履歴データの通話開始日の最大値

### JDBCに関するパラメータ
* url
  - ORacle, PostgreSQL利用時のJDBC URL
  - Tsurugi利用時のendpoint
* user
  - DBのログイン認証に使用するユーザ名
* password
  - DBのログイン認証に使用するパスワード
* isolation.level
  - 使用するトランザクション分離レベル、`READ_COMMITTED` と `SERIALIZABLE`のみが指定可能
  - Tsurugi利用時は無視される
### DBMSタイプ

* dbms.type
  * 使用するDBMSを指定する
  　* POSTGRE_SQL_JDBC: PostgreSQL
  　* ORACLE_JDBC: Oracle
  　* ICEAXE: Tsurugi



### オンラインアプリケーションに関するパラメータ

* master.update.records.per.min
  * 契約マスタの更新頻度、`master.update.thread.count`で指定した各スレッドが1分間にこで指定されたレコード数だけ契約マスタを更新する。-1を指定すると連続で契約マスタを更新する。
* master.update.thread.count=1
  * 契約マスタを更新するスレッドのスレッド数。
* master.delete.insert.records.per.min
  * 契約マスタの追加頻度、`master.delete.insert.thread.count`で指定した各スレッドが1分間にこで指定されたレコード数だけ契約マスタを追加する。-1を指定すると連続で契約マスタを追加する。
* master.delete.insert.thread.count=1
  * 契約マスタを追加するスレッドのスレッド数。
* history.update.records.per.min
  * 通話履歴の更新頻度、`history.update.thread.count`で指定した各スレッドが1分間にこで指定されたレコード数だけ契約マスタを更新する。-1を指定すると連続で通話履歴を更新する。
* history.update.thread.count=1
  * 通話履歴マスタを更新するスレッドのスレッド数。
* history.insert.transaction.per.min
  * 通話履歴の追加頻度、`history.insert.thread.count`で指定した各スレッドが1分間にこで指定された回数だけ契約マスタを更新する。-1を指定すると連続で通話履歴を追加する。
* history.insert.thread.count=1
  * 通話履歴マスタを追加するスレッドのスレッド数。
* history.insert.records.per.transaction
  * 通話履歴の追加時に1回で追加するレコード数。
* skip.database.access=false
  * ツール自体のテスト用のパラメータです。常にfalseを指定して使用してください。

オンラインアプリケーションは、料金計算とは別スレッドで、別のトランザクションとして実行される。
契約マスタの追加、更新、通話履歴の更新は、1レコードのINSERT/UPDATEを1トランザクションとして
実行する。通話履歴の追加は、1トランザクションで、history.insert.records.per.transactionで
指定したレコード数の通話履歴を追加する。

### スレッドに関するパラメータ
* thread.count
  - 電話料金計算処理、およびテストデータ生成時のスレッドのスレッド数
* shared.connection
  - true/falseで指定する。trueのとき電話料金計算処理の各スレッド間でコネクションを共有する。

### CSVに関するパラメータ
* csv.dir
  - `CreateTestDataCsv`コマンド実行時にCSVファイルを出力するディレクトリ。
* max.number.of.lines.history.csv
  - `CreateTestDataCsv`コマンドでCSVファイルを作成するときの、1ファイルに含まれるレコード数。この値が小さいと大量のCSVファイルが生成され取り扱いが困難になるため、生成されるCSVファイル数が1000程度を超えないようにこの値を調整することを推奨します。

### Oracle固有のパラメータ

* oracle.initrans
  - Oracleでテーブル生成するときにのみ使用される
  - historyテーブル生成時のinitransの値を指定する
  - デフォルト値は0
  - 0を指定した場合、テーブル生成時にinitransを指定しない。この場合、initransの値はOracleのデフォルト値になる。
* oracle.sql.loader.path
  - `LoadTestDataCsvToOracle`コマンドは内部でOracleのSQL\*Loaderを呼び出します。SQL*Loaderの実行ファイルをフルパスで指定してください。
* oracle.sql.loader.sid
  - SQL*Loader実行時にしていするOracle SID。Oracle SIDを指定しなくてもSQL\*Loaderを実行可能な環境では指定不要です。
* oracle.create.index.option
  - インデックスおよびプライマリーキー生成時に追加でしていするオプションを指定します。デフォルトでは、`nologging parallel 32`が用いられます。実行環境に応じて適宜指定してください。

### Iceaxe固有のパラメータ
Tsurugi利用時のみ有効なパラメータです。

* transaction.option
  - バッチ実行時のTransaction Option, OCCとLTXのいずれかを指定する。
* use.prepared.tables=false
  - ツール自体のテスト用のパラメータです。常にfalseを指定して使用してください。


### その他のパラメータ
* random.seed=0
  - 乱数のseed、テストデータ生成に関するパラメータと、乱数のseedが同じであれば、同一内容のテストデータが生成される。
* transaction.scope=WHOLE
  - WHOLE/CONTRACTのどちらかを指定する。
  - WHILEが指定されると、料金計算処理全体を1トランザクションとして実行する。ただし、thread.countが2以上でshared.connectionがfalseの場合、各スレッドは独立したトランザクションとなり、バッチ終了時に各スレッドのトランザクションをcommit/rollbackする動作になる。
  - CONTRACTが指定されると、1契約分の料金計算毎にコミットする。
* listen.port
  - マルチノード構成での実行時のサーバのリッスンポート番号

## サンプルバッチの実行

* 前準備
  - OracleまたはPostgreSQLで、サンプルバッチ用のユーザを作成する
  - 設定ファイルを作成する(ここでは、設定ファイルのファイル名を`config.properties`とする)
* 以下のコマンドを実行してテーブルを作成する
```
phone-bill/bin/run CreateTable config.properties
```
* 以下のコマンドで、テストデータを生成する。
```
./phone-bill/bin/run CreateTestData config.properties
```
* 以下のコマンドで、サンプルバッチを実行する。
```
./phone-bill/bin/run PhoneBill config.properties
```

## 大量のテストデータ生成

定量のテストデーを生成する場合、`CreateTestData`コマンドを使用せず、テストデータの`CreateTestDataCsv`コマンドでCSVファイルを生成し、`LoadTestDataCsvToOracle`コマンド
または、`LoadTestDataCsvToPostgreSql`コマンドによりCSVデータをロードすることにより、
高速にテストデータを生成できます。


## 分布関数の指定

### 分布関数について

通話履歴の生成時に、発信者電話番号、受信者電話番号、通話時間の生成に用いる分布関数を指定可能です。

分布関数に`LOGNORMAL`を指定すると、分布関数に`org.apache.commons.math3.distribution LogNormalDistribution`が使用されます。Cconfigで指定された`scale`と`shape`の値は、`org.apache.commons.math3.distribution LogNormalDistribution`に渡されます。作成したいテストデータの性質に合致したshapeとscaleを指定してください。

分館数に`UNIFORM`を指定すると、一様な乱数を生成する乱数生成器`Java.util.Random`が使用されます。`scale`と`shape`の値は
無視されます。

発信者電話番号、受信者電話番号のどちらか、または両方の分布関数に`LOGNORMAL`を指定すると、
契約に紐付く通話履歴数が契約により大きなばらつきがあるテストデータを生成できます。`transaction.scope=CONTRACT`
を指定するとトランザクションの大きさに大きなあるケースのテストを実行できます。

### 統計情報出力

生成したテストデータが意図した分布になっているかの確認のために統計情報を出力します。

テストデータ生成時に通話時間、発信者電話番号、受信者電話番号について以下の情報をログに出力します

* 生成した値の数
* 生成した値の最大値、最小値
* 生成した値の出現頻度(以降単に頻度と記述)の平均値(相加平均)
* 頻度が大きい値のTOP10
* 頻度が小さい値のTOP10

テストデータを生成するコマンド`CreateTestData`の代わりにコマンドを`TestDataStatistics`
実行すると、テストデータを生成せずに統計情報の出力します。また、コマンド`TestDataStatistics`
実行時に、Configの`statistics.output.dir`で指定したディレクトリの下に`statistics`という
名前のディレクトリを作成し、通話時間と電話番号それぞれの生成したすべての値と頻度を記録したCSVファイルを出力します。


## マルチノード構成でのサンプルバッチの実行

現在この機能はメンテナンスされていません。正しく動作しない可能性があります。
オンラインアプリケーションがデータベースに十分な負荷を与えられない場合のために、マルチノード構成でサンプルバッチを実行可能です。

### 概要

マルチノード構成でのサンプルバッチの実行手順は以下のとおりです。

1. マルチノード実行用のサーバを起動する
1. サーバの起動が完了したら、料金計算バッチ用のプロセスと、オンラインアプリ用のプロセスを実行する。以降、料金計算バッチ用のプロセスと、オンラインアプリ用のプロセスの総称としてクライアントプロセスと記述します。
   * シングル構成の場合と異なり、マルチノード構成での実行では料金計算処理のプロセスでは、オンラインアプリ用のスレッドは実行されません。オンラインアプリを実行する場合、オンラインアプリ用のプロセスを実行してください。
   * 料金計算バッチ用のプロセス、オンラインアプリ用のプロセスともにサーバの設定ファイルに従い動作します。
   * 料金珪砂バッチ用のプロセスは1つだけ実行可能です。2つい上のプロセスを起動してもサーバに接続を拒否され起動に失敗します。
   * オンラインアプリ用のプロセスは任意の数だけ起動可能です。
1. サーバのステータスを参照し、起動した料金計算バッチ用のプロセスと、オンラインアプリ用のプロセスが実行可能な状態なことを確認後、実行開始します。
1. サーバのステータスを参照し処理の進捗状況を確認可能です。
2. 処理が終了したら、シャットダウンコマンドを実行して、サーバとクライアントプロセスを終了します。


### サンプルバッチの実行例

#### インストール

サーバおよびクライアントプロセスを実行する各ノードの任意のディレクトリで、生成された`phone-bill.tar.gz` を
展開してインストールする。

```
tar xf phone-bill.tar.gz
```

### サーバの起動

設定ファイルを指定してサーバを起動する

```
./phone-bill/bin/run Server multinode.properties
```

### 料金計算バッチの起動

```
./phone-bill/bin/run PhoneBillClient sv011:1967
```

### オンラインアプリの起動


```
./phone-bill/bin/run OnlineAppClient sv011:1967
```


### サーバのステータス確認

#### ステータスの表示例1

4つのクライアントプロセスがサーバに接続していて初期化中のケース

```
$ ~/phone-bill/bin/run Status sv011:1967
Start         Type       Node            Status    Message from client
-----------------------------------------------------------------------------------
16:42:38      ONLINE_APP sv012           INITIALIZING No message
16:42:38      ONLINE_APP sv013           INITIALIZING No message
16:42:38      ONLINE_APP sv014           INITIALIZING No message
16:42:38      ONLINE_APP sv015           INITIALIZING No message
```


#### ステータスの表示例2

5つのクライアントプロセスがサーバに接続していて、実行待ちの状態。この状態を確認後実行します。

```
$ ~/phone-bill/bin/run Status sv011:1967
Start         Type       Node            Status    Message from client
----------------------------------------------------------------------
16:51:47      ONLINE_APP sv012           READY     Waiting.
16:51:47      ONLINE_APP sv013           READY     Waiting.
16:51:47      ONLINE_APP sv014           READY     Waiting.
16:51:47      ONLINE_APP sv015           READY     Waiting.
16:51:47      PHONEBILL  sv016           READY     Waiting.
```

### オンラインアプリとバッチの実行

#### 実行

次のコマンドでオンラインアプリとバッチを実行します

```
./phone-bill/bin/run Start sv011:1967
```

####  ステータスの確認例1

電話料金バッチのクライアントについては、Queueに入ったマスタレコードの数と未処理のマスタレコードの数が確認できます。
オンラインアプリについては、各クライアントプロセスごとに、処理済み件数が表示されます。

```
$ ~/phone-bill/bin/run Status sv011:1967
Start         Type       Node            Status    Message from client
------------------------------------------------------------------------------------------------------------------------------------
16:51:47      ONLINE_APP sv012           RUNNING   uptime = 0.088 sec, exec count(HistoryInsertApp = 1733, MasterInsertApp = 640909)
16:51:47      ONLINE_APP sv013           RUNNING   uptime = 0.041 sec, exec count(HistoryInsertApp = 93, MasterInsertApp = 261668)
16:51:47      ONLINE_APP sv014           RUNNING   uptime = 0.061 sec, exec count(HistoryInsertApp = 1619, MasterInsertApp = 841022)
16:51:47      ONLINE_APP sv015           RUNNING   uptime = 0.059 sec, exec count(HistoryInsertApp = 212, MasterInsertApp = 473893)
16:51:47      PHONEBILL  sv016           RUNNING   Contracts queue status: total queued taasks = 43612, tasks in queue = 1865
```

####  ステータスの確認例2

料金計算バッチの実行が終了し、オンラインアプリの実行も終了した状態

```
$ ~/phone-bill/bin/run Status sv011:1967
Start         Type       Node            Status    Message from client
------------------------------------------------------------------------------------
16:51:47      ONLINE_APP sv012           SUCCESS   Finished successfully.
16:51:47      ONLINE_APP sv013           SUCCESS   Finished successfully.
16:51:47      ONLINE_APP sv014           SUCCESS   Finished successfully.
16:51:47      ONLINE_APP sv015           SUCCESS   Finished successfully.
16:51:47      PHONEBILL  sv016           SUCCESS   Billings calculated in 59.182 sec
------------------------------------------------------------------------------------
```

### 終了

以下のコマンドで実行中のクライアントプロセスととサーバを終了します

```
 ./phone-bill/bin/run Shutdown sv011:1967
```

## 注意点
* サンプルバッチを複数回実行する場合、実行の前に都度テストデータの生成を行って
ください。テストデータの生成を行わずに、複数回バッチを実行すると、バッチの実行によりデータが書き換わるため、1回目の実行と2回目の条件が変わってしまいます。とくに、オンラインアプリケーションにより大量にデータを追加した場合、処理時間が大幅に増えることがあります。
* テストデータの生成を行うと、通話履歴テーブルと契約マスタの既存データは削除されます。月額利用料金テーブルのデータは削除されずそのまま残ります。
* 分布関数に`UNIFORM`を指定しても、電話番号の頻度に大きなばらつきがあるように見えます。これは当該電話番号の契約期間の長短により電話番号の選択可能性が変わり、電話番号の頻度は電話番号の選択可能性に依存するためです。
