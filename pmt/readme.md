## 電話料金計算バッチのCSV作成機能の利用方法

電話料金計算バッチは以下の手順で実行します。

* テーブルの作成
* テストデータの投入
* バッチの実行

テストデータは条件次第で巨大になるため、高速にテストデータを投入するための
機能が用意されています。具体的にはinsert文でテストデータを投入するのではなく、
csvファイルを生成し生成したcsvファイルをbulkloaderでロードします。
このcsvデータ生成機能を使用します。

csv出力の対象となっているテーブルは履歴(history)テーブルと契約(contracts)テーブルです。

履歴テーブルのDDL
```
create table history (
    caller_phone_number varchar(15) not null,
    recipient_phone_number varchar(15) not null,
    payment_categorty char(1) not null,
    start_time bigint not null,
    time_secs int not null,
    charge int,
    df int not null,
    primary key (caller_phone_number,start_time)
);
```
契約テーブルのDDL
```
create table contracts (
    phone_number varchar(15) not null,
    start_date bigint not null,
    end_date bigint,
    charge_rule varchar(255) not null,
    primary key (phone_number, start_date)
);
```


## ビルド環境と実行環境

以下の環境で開発、動作確認を行っています。同等環境を用意願います。

* OS
  - Ubuntu 18.04LTS または Ubuntu 20.04LTS
* JDK
  - openjdk 11

## ビルドとインストール

ソースコードをgithubから取得

```
git clone git@github.com:project-tsurugi/phone-bill-benchmark.git
```

ブランチ`release-for-pmt`に移動

```
git checkout release-for-pmt
```


ビルド

```
cd src
./gradlew clean distTar
```
ビルドに成功すると、インストール用のアーカイブ`build/distributions/phone-bill.tar`が生成されます。

実行環境の任意のディレクトリで、生成された`phone-bill.tar` を展開してインストールしてください。

```
tar xf phone-bill.tar
```

スクリプト`phone-bill/bin/run`で電話料金計算バッチを起動できます。下の例では、引数を与えて
いないので、エラーとなりusageが表示されています。

```
$ phone-bill/bin/run
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

Following commands must specify a hostname and port number of server.

  PhoneBillClient: Execute phone bill batch client for multienode execution.
  OnlineAppClient: Execute phone bill batch client for multienode execution.
  Status: Reports the execution status of client processes.
  Start: Start execution a phone bill batch and online applications.
  Shutdown: Terminate all client processes and a server process.
```

## 設定ファイル

### 設定ファイルの例

```
# 契約マスタ生成に関するパラメータ
number.of.contracts.records=1000000
duplicate.phone.number.rate=10000
expiration.date.rate=30000
no.expiration.date.rate=50000

# 通話履歴生成に関するパラメータ
number.of.history.records=1000000000

# スレッドに関するパラメータ
thread.count=64

# CSVに関するパラメータ
csv.dir=/tmp/csv
max.number.of.lines.history.csv=1000000

# DBMSに関するパラメータ
url=tcp://localhost:12345
dbms.type=ICEAXE

```

### 契約マスタのテストデータ生成に関するパラメータ
* number.of.contracts.records
  - 契約マスタのレコード数
* duplicate.phone.number.rate
  - 複数の契約を持つ電話番号の割合
* expiration.date.rate
  - 契約終了日を持つ契約の割合
* no.expiration.date.rate
  - 契約終了日を持たない契約の割合

### 通話履歴生成に関するパラメータ

* number.of.history.records
  - 通話履歴のレコード数

### スレッドに関するパラメータ
* thread.count
  - 電話料金計算処理、およびテストデータ生成時のスレッドのスレッド数

### CSVに関するパラメータ
* csv.dir
  - `CreateTestDataCsv`コマンド実行時にCSVファイルを出力するディレクトリ。
* max.number.of.lines.history.csv
  - `CreateTestDataCsv`コマンドでCSVファイルを作成するときの、1ファイルに含まれるレコード数。この値が小さいと大量のCSVファイルが生成され取り扱いが困難になるため、生成されるCSVファイル数が1000程度を超えないようにこの値を調整することを推奨します。

### DBMSに関するパラメータ
* url
  * tsurugi dbへの接続endpoint
* dbms.type=ICEAXE
  * 使用するDBMSを指定する
    * ICEAXE -> Tsurugi
    * ORACLE_JDBC -> Oracle
    * POSTGRE_SQL_JDBC -> PostgreSQL


## csvデータの生成

上記設定ファイル(ファイル名: config.exampleを使用してcsvファイルを生成してみる。

```
phone-bill/bin/run CreateTestDataCsv config.example 
```

arm02で実行すると約6分でcsvファイルが作成されます。
* 約46MByteの履歴データが1000(ファイル名: history-xxx.csv)
* 約37MByteの契約データが1(ファイル名: /contracts.csv)

必要なデータ量、ファイルサイズ、実行環境に応じて、以下のパラメータを変更して使用してください。

* number.of.contracts.records
* number.of.history.records
* thread.count
* csv.dir
* max.number.of.lines.history.csv






