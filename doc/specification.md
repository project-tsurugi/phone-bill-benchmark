# ベンチマークの仕様

本ドキュメントは、バッチの実行中に常にWriteが発生するタイプのバッチの
ベンチマークとして作成する電話料金計算バッチの仕様について記述する。

## テーブル仕様

* 通話履歴
  - 発信者電話番号(PK)
  - 受信者電話番号
  - 料金区分(発信者負担、受信社負担)(PK)
  - 通話開始時刻(PK)
  - 通話時間
  - 通話料金
  - 論理削除フラグ

* 契約マスタ
  - 電話番号(PK)
  - 契約開始日(PK)
  - 契約終了日(NULLあり->契約中で解約の予定がない場合)
  - 料金計算ルール(詳細後述)

* 月額利用料金
  - 電話番号(PK)
  - 対象年月(PK)
  - 基本料金
  - 通話料金
  - 請求金額
  - バッチ実行ID(PK)

## DDL

通話履歴
```
create table history (
  caller_phone_number varchar(15) not null, 
  recipient_phone_number varchar(15) not null, 
  payment_category char(1) not null, 
  start_time timestamp not null, 
  time_secs int not null, 
  charge int, 
  df int not null, 
  primary key (caller_phone_number, payment_category, start_time)
);
create index idx_hst on history(df);
create index idx_st on history(start_time);
create index idx_rp on history(recipient_phone_number, payment_category, start_time);
```

契約マスタ
```
create table contracts (
    phone_number varchar(15) not null,
    start_date date not null,
    end_date date,
    charge_rule varchar(255) not null,
    primary key (phone_number, start_date)
);
```

月額利用料金
```
create table billing (
    phone_number varchar(15) not null,
    target_month date not null,
    basic_charge int not null,
    metered_charge int not null,
    billing_amount int not null,
    batch_exec_id varchar(36) not null,
    primary key(target_month, phone_number, batch_exec_id)
);
```


## データ量とデータサイズ

* データサイズは、テストに使用するサーバのスペックや、テストの効率を考えて別途規定する。
* テストデータ作成時に必要となる諸元についても別途設定する
* 例:
  - 契約マスタ
    * 過去1年分のデータを保持
    * 1000万レコード
    * 契約開始日が異なる同一の電話番号のレコードが全体の1%程度
    * 契約終了日が存在するレコードが50%, そのうち10%が未来日付
  - 通話履歴
    * 過去1年分のデータを保持
    * 1契約に対して、1日あたり平均10回の通話がある
    * 契約ごとの通話回数は対数正規分布に従う
  - 月額利用料金
    * 過去1年分のデータを保持


## バッチ処理概要

* パラメータとして料金計算対象の年月が与えられる
* 月額料金テーブルから、料金計算対象の年月のレコードを削除する
```
delete from billing where target_month = 料金計算対象の年月
```

* 契約マスタから、当該年月に有効な契約があるレコードを抽出する
```
select phone_number, start_date, end_date, charge_rule
from contracts
where start_date <= 当該年月の最終日 and (end_date is null or end_date >= 当該年月の初日)
order by phone_number
```

* 抽出した各レコードに対して以下の処理を繰り返す
  - 通話履歴テーブルから、料金計算対象のレコードの値を取り出す
   * 発信者電話番号が契約マスタの電話番号で同じで、料金区分が発信者負担
   * 受信者電話番号が契約マスタの電話番号で同じで、料金区分が受信者負担
   * 通話開始時刻が、契約マスタの契約開始日～契約終了日の間
```
select caller_phone_number, recipient_phone_number, payment_category, start_time, time_secs, charge, df
from history
where start_time >= 当該年月の初日 and start_time < 当該年月の最終日
and ((caller_phone_number = 発信者電話番号 and payment_category = 発信者負担) or 
  (recipient_phone_number = 受信者電話番号 and payment_category = 受信者負担))
and df = 0
```
  - 料金計算ルール(後述)に従い料金を計算し、通信履歴テーブルの通話料金を更新する(UPDATE)
```
update history
set recipient_phone_number = ?, time_secs = ?, charge = ?, df = ?
where caller_phone_number = ? and payment_category = ? and start_time = ?
```
  - 料金計算ルール(後述)に従い、月額利用料金を更新する(INSERT)
```
insert into billing(phone_number, target_month, basic_charge, metered_charge, billing_amount, batch_exec_id)
values(?, ?, ?, ?, ?, ?)
```

## 料金計算ルール

* 以下のルールがマスタに記録されている。
* DB上のルールの持ち方については別途検討。

### 一通話の料金の計算

* 契約毎に決められた単価 
  - X 秒 当たり Y円
  - Xは10, 20, 30, 60 のいずれか
  - Yは10, 20のいずれか
* 契約毎に決められた例外計算
  - 指定の電話番号への通話は無料
    * 最大3つ指定可能
  - 指定の時間まで無料
    * 5分以内は無料

### 月額料金

* 基本料金
  - 1000円, 2000円, 3000円のいずれか
* 利用料金は当該年月の一通話の料金の和
* 請求金額は基本料金 + 通話料金、ただし以下の例外計算がある
  - 料金計算年月に契約開始日または契約終了日が含まれている場合、基本料金を日割り計算する。
  - 基本料金に通話料金の無料分が含まれているケース
    * 無料料金は基本料金の 50% または 80%
    * 基本料金を日割り計算した場合、無料分も日割り計算する
  - ボリュームディスカウント(契約によりある場合がある)
    * 無料分を除く料金が x 円以上の場合、 x円を超える料金を y %引きとする。
    * x は 1万, 2万, 5万のいずれか
    * y は、10, 20, 30のいずれか
    * 一つの契約に複数のボリュームディスカウントがつくことがある

## 電話番号の再利用について

* 契約が終了した電話番号は、他の契約に再利用されることがある
* 契約終了後、翌月までは電話番号が再利用されないことが保証されている。したがって、月額の料金計算で同一の電話番号の契約が複数存在するケースは考慮不要である。

## オンラインアプリケーション

サンプルバッチ実行中に以下の4種のオンラインアプリケーションを実行する。

###  契約マスタ更新
* 指定の電話番号の契約を取得する   
```
select start_date, end_date, charge_rule
from contracts 
where phone_number = 電話番号 order by start_date
```
* 複数のレコードが選択された場合ランダムに1レコード選択し値を書き換えて書き戻す
  - 契約終了日を削除
  - 契約終了日を設定

```
update contracts set end_date = 契約終了日, charge_rule = 料金計算ルール 
where phone_number = 電話番号 and start_date = 契約開始日
```

### 契約マスタ追加

* 契約マスタにレコードを追加する
```
insert into contracts(phone_number, start_date, end_date, charge_rule) 
values(電話番号, 契約開始日, 契約終了日, 料金計算ルール)
```


### 通話履歴更新

* ランダムに契約を選択(DBアクセスしない)
  * 電話番号と契約開始日
* 指定の契約に紐づく通話履歴を取得する
```
select end_date 
from contracts 
where phone_number = 電話番号 and start_date = 契約開始日

select caller_phone_number, recipient_phone_number, payment_category, start_time,	time_secs, charge, df 
from history 
where start_time >= 契約開始日 and start_time < 契約終了日 and caller_phone_number = 電話番号
```
* 取得した通話履歴からランダムに選択した1レコードを更新する
  - 通話時間を変更する
  - 論理削除する
```
update history
set recipient_phone_number = 受信者電話番号, time_secs = 通話時間, charge = 通話時間, df = 論理削除フラグ
where caller_phone_number = 発信者電話番号 and payment_category = 料金区分 and start_time = 通話開始時刻
```


### 通話履歴追加
* 通話履歴にレコードを追加する
```
insert into history(caller_phone_number, recipient_phone_number, payment_category, start_time, time_secs, charge, df)
values(発信者電話番号, 受信者電話番号, 料金区分, 通話開始時刻, 通話時間, 通話料金, 論理削除フラグ)
```

## 現状の実装について
* バッチ、オンラインアプリケーションともに、Java + JDBC/ Java + Iceaxeで記述
* 料金計算ルールについて
  * 現状の実装では、1種類の料金計算ルールのみを適用
  * 料金計算ルールのバリエーションはアプリケーション側の負荷を高める意図で仕様を作成した。
  * 料金計算をDBMSの外で実行するかぎり料金計算のバリエーションが増えてもDBへの負荷は変わらない

## Tsurugiの制約への対応
現時点ではTsurugiがサポートするSQLにいくつかの制限がある。本バッチに関係する制限と、
本バッチで当該制限にたいしてどのように対応しているのかを記述する。

### NOT NULL / IS NOT NULL が未サポート

Tsurugiがis not nullをサポートしていないため、契約中で解約の予定がない契約について、
NULLではなくLocalDate.MAXを使用している。

### SELECT文のOR条件

Tsurugiはorを使った検索で意図したようにセカンダリインデックスを使用しないのでWHERE条件に
ORをつけたSELECT文を実行する代わりに、条件を変えた二つのSELECT文を実行しその結果を
マージして使用している。

