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
  OnlineAppBench : Execute PhonBill with and without online appications.

FILE: The filename of the configuration file, if not specified, the default value is used.
```

## 設定ファイル

設定ファイルの例: デフォルトの設定値

```
# 料金計算に関するパラメータ
target.month=2020-12-01

# 契約マスタ生成に関するパラメータ
number.of.contracts.records=1000
duplicate.phone.number.ratio=10
expiration.date.rate=30
no.expiration.date.rate=50
min.date=2010-11-11
max.date=2021-03-01

# 通話履歴生成に関するパラメータ
number.of.history.records=1000

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
* duplicate.phone.number.ratio
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
* thread.count=1
  - 電話料金計算処理のスレッドのスレッド数
* shared.connection=true
  - true/falseで指定する。trueのとき電話料金計算処理の各スレッド間でコネクションを共有する。

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

### 注意点

* サンプルバッチを複数回実行する場合、実行の前に都度テストデータの生成を行って
下さい。テストデータの生成を行わずに、複数回バッチを実行すると、バッチの実行によりデータが書き換わるため、1回目の実行と2回目の条件が変わってしまいます。特に、オンラインアプリケーションにより大量にデータを追加した場合、処理時間が大幅に増えることがあります。
* テストデータの生成を行うと、通話履歴テーブルと契約マスタの既存データは削除されます。月額利用料金テーブルのデータは削除されずそのまま残ります。


