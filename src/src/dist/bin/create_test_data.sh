#!/bin/bash -e

if [ $# -eq 0 ]; then
  echo usage: $0 config_files
  exit 1
fi

if [[ -z "$TSURUGI_HOME" ]]; then
  echo "Error: The environment variable TSURUGI_HOME is not set. Please set TSURUGI_HOME."
fi

BIN_DIR=$(cd $(dirname $0); pwd)

ext=`date "+%Y%m%d-%H%M%S"`.log
export JAVA_OPTS="$JAVA_OPTS -Dcom.tsurugidb.tsubakuro.jniverify=false \
 -Dlogback.configurationFile=$BIN_DIR/../etc/logback.xml \
 -Dlogfile=$BIN_DIR/../logs/create_test_data-$ext"

$BIN_DIR/run CreateTestData "$@"

