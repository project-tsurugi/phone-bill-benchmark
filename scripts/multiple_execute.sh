#!/bin/bash -uex
set -ue

if [ $# -eq 0 ]; then
  echo usage: $0 config_files
  exit 1
fi

BIN_DIR=$(cd $(dirname $0); pwd)

. $BIN_DIR/env.sh


# execute phone-bill

sed "s!log_location=.*!log_location=$TSURUGI_LOG_DIR!" $BIN_DIR/etc/phone-bill.ini.template > $BIN_DIR/etc/phone-bill.ini

export DB_INIT_CMD=$BIN_DIR/tinit.sh
export JAVA_OPTS="$JAVA_OPTS -Dcom.tsurugidb.tsubakuro.jniverify=false -Dlogback.configurationFile=$BIN_DIR/etc/logback.xml -Dlogfile=$LOG_DIR/multiple-exec-`date "+%Y%m%d_%H%M%S"`.log"

$INSTALL_DIR/phone-bill/bin/run MultipleExecute "$@"


