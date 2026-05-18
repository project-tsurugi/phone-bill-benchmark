#!/bin/bash
set -euo pipefail

CONF="${CONF_PATH:-phone-bill/conf/batch-only}"
S3_BUCKET="${S3_BUCKET:?S3_BUCKET is required}"
OUTPUT_MODE="${OUTPUT_MODE:-tsurugidb}"
DUMP_DIR="${DUMP_DIR:-dump}"

append_sysprop() {
  local env_name="$1"
  local prop_name="$2"
  local value="${!env_name:-}"

  if [ -n "$value" ]; then
    JAVA_OPTS="${JAVA_OPTS:-} -D${prop_name}=${value}"
  fi
}

# conf名をS3プレフィックスに使う（例: batch-only）
CONF_NAME=$(basename "$CONF")
# 同じconfで複数回実行してもS3上の古いpartと混ざらないよう、未指定時はタイムスタンプ付きにする
S3_PREFIX="${S3_PREFIX:-${CONF_NAME}/$(date +%Y%m%dT%H%M%S)}"

append_sysprop CREATE_TEST_DATA_THREAD_COUNT phone-bill.create.test.data.thread.count
export JAVA_OPTS="${JAVA_OPTS:-} -Xmx${JAVA_XMX:-30g}"

upload_dump() {
  echo "Uploading to s3://${S3_BUCKET}/${S3_PREFIX}/"
  aws s3 cp "${DUMP_DIR}/" "s3://${S3_BUCKET}/${S3_PREFIX}/" --recursive
}

if [ "$OUTPUT_MODE" = "parquet-direct" ]; then
  rm -rf "$DUMP_DIR"
  phone-bill/bin/create_test_data_parquet.sh "$CONF" "$DUMP_DIR"
  upload_dump
  echo "Done."
  exit 0
fi

if [ "$OUTPUT_MODE" != "tsurugidb" ]; then
  echo "Unknown OUTPUT_MODE: $OUTPUT_MODE" >&2
  exit 1
fi

# TsurugiDB 起動
echo "Starting TsurugiDB..."
tgctl start

# 起動待ち
echo "Waiting for TsurugiDB..."
until tgctl status > /dev/null 2>&1; do
  sleep 2
done
echo "TsurugiDB is ready."

# テーブル作成・データ生成・実行
phone-bill/bin/create_table.sh "$CONF"
phone-bill/bin/create_test_data.sh "$CONF"
phone-bill/bin/execute.sh "$CONF"

# ダンプ（dump/ が空でないとエラーになるため事前に削除）
rm -rf "$DUMP_DIR"
/usr/lib/tsurugi/bin/tgdump -c ipc:tsurugi \
    history billing contracts --to "$DUMP_DIR"

# S3 アップロード
upload_dump

echo "Done."
