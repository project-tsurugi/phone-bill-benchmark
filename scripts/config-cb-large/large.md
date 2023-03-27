# phone-bill-benchmark large

| テーブル名 | 行数       | サイズ(KB) | CRUD | 備考                               |
|------------|-----------:|-----------:|------|------------------------------------|
| history    | 30,000,000 | 7,987,200  | RU   | バッチ実行前のサイズは 6,520,832KB |
| contracts  | 10,000      | 904        | R    |                                    |
| billing    | 5,258      | 1,400      | C    | バッチ実行後の値                   |
| 合計       | 30,015,258 | 7,989,504  |      |                                    |

詳細は[ここ](https://github.com/project-tsurugi/phone-bill-benchmark/blob/master/scripts/data_size.md)を参照
