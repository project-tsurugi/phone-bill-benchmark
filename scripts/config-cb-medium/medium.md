# phone-bill-benchmark medium

| テーブル名 | 行数       | サイズ(KB) | CRUD | 備考                               |
|------------|-----------:|-----------:|------|------------------------------------|
| history    | 3,000,000  | 831488     | RU   | バッチ実行前のサイズは 652,288KB   |
| contracts  | 1,000      | 128        | R    |                                    |
| billing    | 524        | 576        | C    | バッチ実行後の値                   |
| 合計       | 3,001,524  | 832,192    |      |                                    |

詳細は[ここ](https://github.com/project-tsurugi/phone-bill-benchmark/blob/master/scripts/data_size.md)を参照
