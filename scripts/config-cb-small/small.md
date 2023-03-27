# phone-bill-benchmark small

| テーブル名 | 行数       | サイズ(KB) | CRUD | 備考                               |
|------------|-----------:|-----------:|------|------------------------------------|
| history    | 300,000    | 87040      | RU   | バッチ実行前のサイズは 66,560KB    |
| contracts  | 1,000      | 128        | R    |                                    |
| billing    | 524        | 416        | C    | バッチ実行後の値                   |
| 合計       | 301,524    | 87,584     |      |                                    |

詳細は[ここ](https://github.com/project-tsurugi/phone-bill-benchmark/blob/master/scripts/data_size.md)を参照
