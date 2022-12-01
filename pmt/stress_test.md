# ストレステスト用ツール

システムテスト実施時にTsurguiに負荷をかける目的で使用する
スクリプトについての説明

## JDKについて

* Java11のJDKが必要です
* 環境変数JAVA_HOMEの設定して下さい


## インストール

1. phone-bill-benchmarkを展開する
2. phone-bill-benchmark/scripts/install.sh を実行

## 実行

バッチとオンラインアプリを同時に実行
```
cd phone-bill-benchmark/scripts
./run_batch_continuously.sh config/stress_test.properties 
```
バッチのみ実行
```
cd phone-bill-benchmark/scripts
./run_batch_continuously.sh config/OCC-CONTRACT-T2.properties
```

オンラインアプリのみ実行
```
cd phone-bill-benchmark/scripts
./run_online_app_continuously.sh config/stress_test.properties 
```
## 注意事項

* 各コマンドの引数は電話料金計算バッチの設定ファイルです。
必要に応じて修正してご使用下さい。

* 上記の各コマンドは、以下のテーブルの内容を初期化します。
既存のデータは失われるのでご注意願います。
  * contracts
  * history
  * billing
 

