# データサイズ

## データサイズの測定

現状ではTsurguiにテーブル単位のデータサイズを取得する機能がない。
データサイズを示す指標として、PostgreSQLに同等のテーブルを作成し、
同内容のデータを投入したときのテーブルサイズを用いる。

行数とデータサイズの測定に使用するクエリは以下の通り

```
select count(*) from table_name;
select pg_size_pretty(pg_total_relation_size('table_name'));
```

## CBのデータサイズ(初期データ)

* 電話料金計算バッチのテストデータ生成機能は初期データをhistory, contractsの2テーブルに書き込む。
* 電話料金計算バッチは、contractsテーブルの約50%を更新し、実行結果をbillingテーブルに書き込む
* 電話料金計算バッチではCB用にtiny, small ,medium, largeの4サイズの設定を用意している。
* それぞれの設定について、データの行数とサイズをを下表に示す。

| 設定   | テーブル  | 行数       | サイズ(KB) | 備考                               |
|--------|-----------|-----------:|-----------:|------------------------------------|
| tiny   | history   | 30,000     | 9,344       | バッチ実行前のサイズは 6,880KB     |
|        | contracts | 1,000      | 128        |                                    |
|        | billing   | 524        | 280        | バッチ実行後の値                   |
| small  | history   | 300,000    | 87,040      | バッチ実行前のサイズは 66,560KB    |
|        | contracts | 1,000      | 128        |                                    |
|        | billing   | 524        | 416        | バッチ実行後の値                   |
| medeum | history   | 3,000,000  | 831,488     | バッチ実行前のサイズは 652,288KB   |
|        | contracts | 1,000      | 128        |                                    |
|        | billing   | 524        | 576        | バッチ実行後の値                   |
| large  | history   | 30,000,000 | 7,987,200  | バッチ実行前のサイズは 6,520,832KB |
|        | contracts | 1,000      | 904        |                                    |
|        | billing   | 5,258      | 1,400      | バッチ実行後の値                   |

※ 各テーブルのサイズと、billingテーブルの行数は特定条件下で測定した測定値である。乱数要素があるため、同一の設定で動かしても本表と異なる測定結果になり得る。


