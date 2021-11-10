# サンプルバッチの使用方法


## ビルド環境と実行環境

以下の環境で開発、動作確認を行っています。同等環境を用意願います。

* OS
 - Ubuntu 18.04LTS
* JDK
 - openjdk 11.0.7
* DBMS
  - PostgreSQL 13.2
  - Oracle Database 19c Enterprise Edition Release 19.0.0.0.0

## ビルドとインストール

ソースコードをgithubから取得

```
git clone git@github.com:project-tsurugi/phone-bill-benchmark.git
```

ビルド

```
cd src
./gradlew clean distTar
```
ビルドに成功すると、インストール用のアーカイブ`build/distributions/phone-bill.tar`が生成される。

実行環境の任意のディレクトリで、生成された`phone-bill.tar` を展開してインストールする。

```
tar xf phone-bill.tar
```

スクリプト`phone-bill/bin/run`でサンプルバッチを起動する。下の例では、引数を与えて
いないので、エラーとなりusageが表示される。

```
$ ./phone-bill/bin/run
ERROR: No argument is specified.

USAGE: run COMMAND [FILE]

COMMAND: Following commands available

  CreateTable : Create tables
  CreateTestData : Create test data
  PhoneBill : Execute phone bill batch.
  ThreadBench : Execute PhonBill with multiple thread counts
  OnlineAppBench : Execute PhonBill with and without online applications.
  TestDataStatistics: Create test data statistics without test data.

FILE: The filename of the configuration file, if not specified, the default value is used.
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

# オンラインアプリケーションに関するパラメータ
master.update.records.per.min=0
master.insert.reccrds.per.min=0
history.update.records.per.min=0
history.insert.transaction.per.min=0
history.insert.records.per.transaction=1

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

# その他のパラメータ
random.seed=0
transaction.scope=WHOLE

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

### 通話履歴のテストデータ生成に関するパラメータ

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
  - JDBC URL
* user
  - DBのログイン認証に使用するユーザ名
* password
  - DBのログイン認証に使用するパスワード
* solation.level
  - 使用するトランザクション分離レベル、`READ_COMMITTED` と `SERIALIZABLE`のみが指定可能

### オンラインアプリケーションに関するパラメータ

* master.update.records.per.min
  - 契約マスタの更新頻度、1分間にこで指定されたレコード数だけ契約マスタを更新する。
* master.insert.reccrds.per.min
  - 契約マスタの追加頻度、1分間にこで指定されたレコード数だけ契約マスタを追加する。
* history.update.records.per.min
  - 通話履歴の更新頻度、1分間にこで指定されたレコード数だけ契約マスタを更新する。
* history.insert.transaction.per.min
  - 通話履歴の追加頻度、1分間にこで指定されたレコード回数だけ契約マスタを更新する。
* history.insert.records.per.transaction
  - 通話履歴の追加時に1回で追加するレコードする。

オンラインアプリケーションは、料金計算とは別スレッドで、別のトランザクションとして実行される。
契約マスタの追加、更新、通話履歴の更新は、1レコードのINSERT/UPDATEを1トランザクションとして
実行する。通話履歴の追加は、1トランザクションで、history.insert.records.per.transactionで
指定したレコード数の通話履歴を追加する。

### スレッドに関するパラメータ
* thread.count
  - 電話料金計算処理のスレッドのスレッド数
  - CSVデータ生成処理のスレッド数
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

### その他のパラメータ
* random.seed=0
  - 乱数のseed、テストデータ生成に関するパラメータと、乱数のseedが同じであれば、同一内容のテストデータが生成される。
* transaction.scope=WHOLE
  - WHOLE/CONTRACTのどちらかを指定する。
  - WHILEが指定されると、料金計算処理全体を1トランザクションとして実行する。ただし、thread.countが2以上でshared.connectionがfalseの場合、各スレッドは独立したトランザクションとなり、バッチ終了時に、各スレッドのトランザクションをcommit/rollbackする動作になる。
  - CONTRACTが指定されると、1契約分の料金計算毎にコミットする。

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
高速にテストデータを生成することができます。


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
名前のディレクトリを作成し、通話時間と電話番号それぞれについて、生成したすべての値と頻度を記録したCSVファイルを出力します。

## 注意点

* サンプルバッチを複数回実行する場合、実行の前に都度テストデータの生成を行って
下さい。テストデータの生成を行わずに、複数回バッチを実行すると、バッチの実行によりデータが書き換わるため、1回目の実行と2回目の条件が変わってしまいます。特に、オンラインアプリケーションにより大量にデータを追加した場合、処理時間が大幅に増えることがあります。
* テストデータの生成を行うと、通話履歴テーブルと契約マスタの既存データは削除されます。月額利用料金テーブルのデータは削除されずそのまま残ります。
* 分布関数に`UNIFORM`を指定しても、電話番号の頻度に大きなばらつきがあるように見えます。これは当該電話番号の契約期間の長短により電話番号の選択可能性が変わり、電話番号の頻度は電話番号の選択可能性に依存するためです。

