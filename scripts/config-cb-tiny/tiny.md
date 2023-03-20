# phone-bill-benchmark tiny

| テーブル名 | 行数       | サイズ(KB) | CRUD | 備考                               |
|------------|-----------:|-----------:|------|------------------------------------|
| history    | 30,000     | 9344       | RU   | バッチ実行前のサイズは 6,880KB     |
| contracts  | 1,000      | 128        | R    |                                    |
| billing    | 524        | 280        | C    | バッチ実行後の値                   |
| 合計       | 31,524     | 9,752      |      |                                    |

詳細は[ここ](https://github.com/project-tsurugi/phone-bill-benchmark/blob/master/scripts/data_size.md)を参照
