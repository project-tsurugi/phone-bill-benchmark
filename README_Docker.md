
# イメージをビルド
```sh
docker build --platform linux/amd64 -t phone-bill-benchmark .
```

# コンテナを実行
```sh
docker run --platform linux/amd64 -it --shm-size=2gb  phone-bill-benchmark
```
※ shm-sizeが小さいとcreate_test_dataが実行できない

# データ生成 (コンテナ内)

```sh
phone-bill/bin/create_table.sh phone-bill/conf/batch-only
phone-bill/bin/create_test_data.sh phone-bill/conf/batch-only
phone-bill/bin/execute.sh phone-bill/conf/batch-only
```